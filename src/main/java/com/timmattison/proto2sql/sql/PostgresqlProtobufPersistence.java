package com.timmattison.proto2sql.sql;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UnmodifiableLazyStringList;
import com.googlecode.protobuf.format.JsonFormat;
import org.postgresql.jdbc4.Jdbc4Array;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by timmattison on 11/4/14.
 */
public class PostgresqlProtobufPersistence implements ProtobufPersistence {
    private static final String DEFAULT_ID_NAME = "id";
    private static final String VALUES = " VALUES ";
    private static final String CAST = "CAST(";
    private static final String AS = " AS ";
    private static final String MESSAGE = "MESSAGE";
    private static final String ENUM = "ENUM";
    private static final String TEXT = "text";
    private static final String INSERT_INTO = "INSERT INTO ";

    private static final String SELECT_FROM = "SELECT * FROM ";
    private static final String WHERE = " WHERE ";
    private static final String VARIABLE = "?";
    private static final String UPDATE = "UPDATE ";
    private static final String SET = " SET ";
    private static final String EQUALS = " = ";

    private final DataSource dataSource;

    @Inject
    public PostgresqlProtobufPersistence(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<Message> select(String idName, String id, Message.Builder builder) throws SQLException, JsonFormat.ParseException {
        // Get the name of the table that this protobuf resides in in the database
        String tableName = getTableName(builder.getDescriptorForType());

        if (id != null) {
            idName = setDefaultIdIfNecessary(idName);
        }

        // Get a connection to the database and prepare the statement
        Connection connection = dataSource.getConnection();

        try {
            // Get the result set
            ResultSet resultSet = getResultSet(connection, idName, id, tableName);

            List<Message> messages = new ArrayList<Message>();

            // Loop through the results
            while (resultSet.next()) {
                // Clear out the builder and get the builder's descriptor
                builder.clear();
                Descriptors.Descriptor descriptor = builder.getDescriptorForType();

                // Loop through all of the fields in this object
                for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
                    // Get the field's name
                    String fieldName = fieldDescriptor.getName();

                    // Get the field's type
                    Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();

                    // What type is this field?
                    if (fieldType.equals(Descriptors.FieldDescriptor.Type.ENUM)) {
                        // It is an enum.  Convert the string to the actual enum.
                        builder.setField(fieldDescriptor, fieldDescriptor.getEnumType().findValueByName(resultSet.getString(fieldName)));
                    } else if (fieldType.equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
                        // It is a message.  Convert it from JSON to a protobuf.

                        if (fieldDescriptor.isRepeated()) {
                            // Could be a normal type or could be an array.  Get the object.
                            Object fieldObject = resultSet.getObject(fieldName);

                            Jdbc4Array jdbc4Array = (Jdbc4Array) fieldObject;
                            List<Object> objectList = Arrays.asList((Object[]) jdbc4Array.getArray());

                            Message.Builder internalBuilder = builder.newBuilderForField(fieldDescriptor);

                            for (Object jsonObject : objectList) {
                                internalBuilder.clear();
                                JsonFormat.merge((String) jsonObject, internalBuilder);
                                builder.addRepeatedField(fieldDescriptor, internalBuilder.build());
                            }
                        } else {
                            DescriptorProtos.FieldDescriptorProto.Builder internalBuilder = fieldDescriptor.toProto().newBuilderForType();
                            JsonFormat.merge(resultSet.getString(fieldName), internalBuilder);
                            builder.setField(fieldDescriptor, internalBuilder.build());
                        }
                    } else {
                        // Could be a normal type or could be an array.  Get the object.
                        Object fieldObject = resultSet.getObject(fieldName);

                        // Is it an array?
                        if (fieldObject instanceof Jdbc4Array) {
                            // Yes, it is an array.  Convert it and set it in the builder.
                            Jdbc4Array jdbc4Array = (Jdbc4Array) fieldObject;
                            List<Object> objectList = Arrays.asList((Object[]) jdbc4Array.getArray());
                            builder.setField(fieldDescriptor, objectList);
                        } else {
                            // No, just set it in the builder.
                            builder.setField(fieldDescriptor, fieldObject);
                        }
                    }
                }

                // Add the built message to the message list
                messages.add(builder.build());
            }

            return messages;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public ResultSet getResultSet(Connection connection, String idName, String id, String tableName) throws SQLException {
        // Start building the SELECT statement from the table
        StringBuilder selectSql = new StringBuilder();
        selectSql.append(SELECT_FROM);
        selectSql.append(tableName);

        // If there is an ID add a WHERE clause
        idWhereClause(idName, selectSql);

        PreparedStatement preparedStatement = connection.prepareStatement(selectSql.toString());

        // Do we have an ID?
        if (idName != null) {
            // Yes, add it as a parameter
            preparedStatement.setString(1, id);
        }

        // Execute the query
        return preparedStatement.executeQuery();
    }

    /**
     * Adds a WHERE clause if the ID name value is not NULL.  The WHERE clause is bound to a parameter, not an actual
     * value.
     *
     * @param idName
     * @param stringBuilder
     */
    private void idWhereClause(String idName, StringBuilder stringBuilder) {
        // Do we have an ID name value?
        if (idName != null) {
            // Yes, build the parameterized WHERE clause
            stringBuilder.append(WHERE);
            stringBuilder.append(idName);
            stringBuilder.append(" = ");
            stringBuilder.append(VARIABLE);
        }
    }

    private static String setDefaultIdIfNecessary(String idName) {
        // Did they specify the ID name?
        if (idName == null) {
            // No, use the default ID name
            idName = DEFAULT_ID_NAME;
        }

        return idName;
    }

    private static String getTableName(Descriptors.Descriptor descriptor) {
        // Get the table name by removing "domain." from the full name and then by replacing all dots with underscores
        return descriptor.getFullName().replaceFirst("domain.", "").replaceAll("\\.", "_");
    }

    public void insert(Message message, String idName, String id) throws SQLException {
        // Set the default ID name if necessary
        idName = setDefaultIdIfNecessary(idName);

        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Start building the INSERT statement
        StringBuilder insertSql = new StringBuilder();
        insertSql.append(INSERT_INTO);
        insertSql.append(protobufTypeName);
        insertSql.append(" ");

        // Build the field name list and the field value placeholders separately.  They are combined later.
        StringBuilder fieldNames = new StringBuilder();
        StringBuilder fieldPlaceholders = new StringBuilder();

        fieldNames.append("(");
        fieldPlaceholders.append("(");

        String separator = "";

        // Is the ID name a field that is not already in the object?
        if (idSpecifiedAndNotNativeField(idName, descriptor)) {
            // Yes, this means that it could be a child object.  We need to add in the ID field since it is not in the protobuf.
            fieldNames.append(separator);
            fieldPlaceholders.append(separator);

            fieldNames.append(idName);
            fieldPlaceholders.append(VARIABLE);

            separator = ", ";
        }

        // Loop through the fields
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            // Get the name of the field type
            String typeName = field.getType().name();

            // Get the name and type name of the current field
            String name = field.getName();

            fieldNames.append(separator);
            fieldPlaceholders.append(separator);

            // Do the first part of ENUM casting if necessary
            castEnumPart1(fieldPlaceholders, typeName);

            // Add the field name
            safeAddFieldName(fieldNames, name);

            // Add the placeholder
            fieldPlaceholders.append(VARIABLE);

            // Do the second part of ENUM casting if necessary
            castEnumPart2(fieldPlaceholders, field, typeName);

            separator = ", ";
        }

        // Close up the different parts of the INSERT statement
        fieldNames.append(")");
        fieldPlaceholders.append(")");

        // Combine them
        insertSql.append(fieldNames);
        insertSql.append(VALUES);
        insertSql.append(fieldPlaceholders);

        // Get a connection to the database and prepare the statement
        Connection connection = dataSource.getConnection();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql.toString());

            int counter = 1;

            // Is the ID name a field that is not already in the object?
            if (idSpecifiedAndNotNativeField(idName, descriptor)) {
                // Yes, this is always the first parameter that needs to be bound
                preparedStatement.setObject(counter, id);

                // Move onto the next position
                counter++;
            }

            // Bind all of the parameters
            bindParameters(message, descriptor, connection, preparedStatement, counter);

            // Execute the query
            preparedStatement.execute();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void update(Message message, String idName, String id) throws SQLException {
        // Did they specify an ID?
        if (id == null) {
            // No, they cannot do an update without an ID

            // Is there an ID field?
            Descriptors.FieldDescriptor idField = message.getDescriptorForType().findFieldByName(DEFAULT_ID_NAME);

            if (idField == null) {
                throw new UnsupportedOperationException("ID cannot be NULL on an update");
            }

            // Find the ID
            id = (String) message.getField(idField);
        }

        // Set the default ID name if necessary
        idName = setDefaultIdIfNecessary(idName);

        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Start building the UPDATE statement
        StringBuilder updateSql = new StringBuilder();
        updateSql.append(UPDATE);
        updateSql.append(protobufTypeName);
        updateSql.append(SET);

        String separator = "";

        // Loop through all of the fields
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            // Get the field type
            String typeName = field.getType().name();

            // Get the name and type name of the current field
            String name = field.getName();

            updateSql.append(separator);

            // Add the field name
            safeAddFieldName(updateSql, name);

            updateSql.append(EQUALS);

            // Do the first part of ENUM casting if necessary
            castEnumPart1(updateSql, typeName);

            updateSql.append(VARIABLE);

            // Do the second part of ENUM casting if necessary
            castEnumPart2(updateSql, field, typeName);

            separator = ", ";
        }

        // Add the WHERE clause
        idWhereClause(idName, updateSql);

        // Get a connection to the database and prepare the statement
        Connection connection = dataSource.getConnection();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql.toString());

            int counter = 1;

            // Loop through all of the parameters
            counter = bindParameters(message, descriptor, connection, preparedStatement, counter);

            // Bind the ID as the last parameter
            preparedStatement.setObject(counter, id);

            // Execute the query
            preparedStatement.execute();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private static void safeAddFieldName(StringBuilder stringBuilder, String name) {
        // Add the field name with double quotes
        stringBuilder.append("\"");
        stringBuilder.append(name);
        stringBuilder.append("\"");
    }

    private static boolean idSpecifiedAndNotNativeField(String idName, Descriptors.Descriptor descriptor) {
        // Return true if the ID name is not NULL and that field name is not in the descriptor
        return (idName != null) && (descriptor.findFieldByName(idName) == null);
    }

    private static void castEnumPart2(StringBuilder stringBuilder, Descriptors.FieldDescriptor field, String typeName) {
        // Is this an ENUM?
        if (ENUM.equals(typeName)) {
            // Yes, finish the SQL CAST
            stringBuilder.append(AS);
            stringBuilder.append(field.getEnumType().getName());
            stringBuilder.append(")");
        }
    }

    private static void castEnumPart1(StringBuilder stringBuilder, String typeName) {
        // Is this an ENUM?
        if (ENUM.equals(typeName)) {
            // Yes, we'll need a SQL CAST here
            stringBuilder.append(CAST);
        }
    }

    private static int bindParameters(Message message, Descriptors.Descriptor descriptor, Connection connection, PreparedStatement preparedStatement, int counter) throws SQLException {
        // Loop through all of the fields
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            // Get the type name
            String typeName = fieldDescriptor.getType().name();

            // Get the raw object
            Object field = message.getField(fieldDescriptor);

            // What type of parameters is this?
            if (MESSAGE.equals(typeName)) {
                // It is a message, just convert it to JSON
                addMessageToPreparedStatement(connection, message, preparedStatement, counter, fieldDescriptor, field);
            } else if (ENUM.equals(typeName)) {
                // It is an enum, convert it to a string
                preparedStatement.setObject(counter, field.toString());
            } else if (field instanceof UnmodifiableLazyStringList) {
                // It is a list, convert it to a JDBC array
                UnmodifiableLazyStringList stringList = (UnmodifiableLazyStringList) field;
                Array sqlArray = connection.createArrayOf(TEXT, stringList.toArray());
                preparedStatement.setArray(counter, sqlArray);
            } else {
                // It is something else, just use it directly
                preparedStatement.setObject(counter, field);
            }

            // Move on to the next field
            counter++;
        }

        return counter;
    }

    private static void addMessageToPreparedStatement(Connection connection, Message parentMessage, PreparedStatement preparedStatement, int counter, Descriptors.FieldDescriptor fieldDescriptor, Object message) throws SQLException {
        // Is the message repeated?
        if (fieldDescriptor.isRepeated()) {
            // Yes, handle the repeated messages
            int count = parentMessage.getRepeatedFieldCount(fieldDescriptor);

            List<String> childMessages = new ArrayList<String>();

            for (int loop = 0; loop < count; loop++) {
                childMessages.add(JsonFormat.printToString((Message) parentMessage.getRepeatedField(fieldDescriptor, loop)));
            }

            Array sqlArray = connection.createArrayOf(TEXT, childMessages.toArray());
            preparedStatement.setArray(counter, sqlArray);
        } else {
            // No, just put one message
            preparedStatement.setObject(counter, JsonFormat.printToString((Message) message));
        }
    }

}