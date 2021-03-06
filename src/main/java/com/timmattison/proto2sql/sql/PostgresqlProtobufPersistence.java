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
public class PostgresqlProtobufPersistence extends AbstractProtobufPersistence implements ProtobufPersistence {
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
    private static final String DELETE_FROM = "DELETE FROM ";
    private static final String ROLLBACK = "ROLLBACK";
    private static final String COMMIT = "COMMIT";

    private final DataSource dataSource;

    private Connection currentConnection;

    @Inject
    public PostgresqlProtobufPersistence(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<Message> innerSelect(String idName, String id, Message.Builder builder, String tableName) throws SQLException, JsonFormat.ParseException {
        // Get a connection to the database and prepare the statement.
        Connection connection = getNewOrExistingConnection();

        try {
            // Get the result set
            ResultSet resultSet = getResultSet(connection, idName, id, tableName);

            List<Message> messages = new ArrayList<Message>();

            // Loop through the results
            while (resultSet.next()) {
                // Clear out the builder and build a new protobuf
                builder.clear();
                buildProtobuf(builder, resultSet);

                // Add the built message to the message list
                messages.add(builder.build());
            }

            return messages;
        } finally {
            closeIfNecessary(connection);
        }
    }

    private void buildProtobuf(Message.Builder builder, ResultSet resultSet) throws SQLException, JsonFormat.ParseException {
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

                // Create a new builder for this internal message
                Message.Builder internalBuilder = builder.newBuilderForField(fieldDescriptor);

                if (fieldDescriptor.isRepeated()) {
                    // Could be a normal type or could be an array.  Get the object.
                    Object fieldObject = resultSet.getObject(fieldName);

                    Jdbc4Array jdbc4Array = (Jdbc4Array) fieldObject;
                    List<Object> objectList = Arrays.asList((Object[]) jdbc4Array.getArray());

                    // Loop through all of the objects
                    for (Object jsonObject : objectList) {
                        // Clear the builder
                        internalBuilder.clear();

                        // Create a protobuf using the JSON and the internal builder
                        JsonFormat.merge((String) jsonObject, internalBuilder);

                        // Add built protobuf in the repeated field
                        builder.addRepeatedField(fieldDescriptor, internalBuilder.build());
                    }
                } else {
                    // Create a protobuf using the JSON and the internal builder
                    JsonFormat.merge(resultSet.getString(fieldName), internalBuilder);

                    // Set the field to the built protobuf
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
    }

    @Override
    public void resultSetToProtobuf(Message.Builder builder, ResultSet resultSet) throws SQLException, JsonFormat.ParseException {
        // Get the descriptor
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
            stringBuilder.append("\"");
            stringBuilder.append(idName);
            stringBuilder.append("\" = ");
            stringBuilder.append(VARIABLE);
        }
    }

    @Override
    public void innerInsert(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException {
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

        /* TODO - See if we still need this
        // Is the ID name a field that is not already in the object?
        if (idSpecifiedAndNotNativeField(idName, descriptor)) {
            // Yes, this means that it could be a child object.  We need to add in the ID field since it is not in the protobuf.
            fieldNames.append(separator);
            fieldPlaceholders.append(separator);

            fieldNames.append(fieldDescriptor.getName());
            fieldPlaceholders.append(VARIABLE);

            separator = ", ";
        }
        */

        // Loop through the fields
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
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

        // Get a connection to the database and prepare the statement.
        Connection connection = getNewOrExistingConnection();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql.toString());

            int counter = 1;

            /* TODO - See if we still need this
            // Is the ID name a field that is not already in the object?
            if (idSpecifiedAndNotNativeField(idName, descriptor)) {
                // Yes, this is always the first parameter that needs to be bound
                preparedStatement.setObject(counter, id);

                // Move onto the next position
                counter++;
            }
            */

            // Bind all of the parameters
            bindParameters(message, message.getDescriptorForType(), connection, preparedStatement, counter);

            // Execute the query
            preparedStatement.execute();
        } finally {
            closeIfNecessary(connection);
        }
    }

    @Override
    protected void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException {
        innerUpdate(message, fieldDescriptor, protobufTypeName, null);
    }

    @Override
    protected void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName, Object previousId) throws SQLException {
        // Start building the UPDATE statement
        StringBuilder updateSql = new StringBuilder();
        updateSql.append(UPDATE);
        updateSql.append(protobufTypeName);
        updateSql.append(SET);

        String separator = "";

        // Loop through all of the fields
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
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
        idWhereClause(fieldDescriptor.getName(), updateSql);

        // Get a connection to the database and prepare the statement.
        Connection connection = getNewOrExistingConnection();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql.toString());

            int counter = 1;

            // Loop through all of the parameters
            counter = bindParameters(message, message.getDescriptorForType(), connection, preparedStatement, counter);

            // Bind the ID as the last parameter
            if (previousId != null) {
                preparedStatement.setObject(counter, previousId);
            } else {
                preparedStatement.setObject(counter, message.getField(fieldDescriptor));
            }

            // Execute the query
            preparedStatement.execute();
        } finally {
            closeIfNecessary(connection);
        }
    }

    private void closeIfNecessary(Connection connection) throws SQLException {
        // Is there a connection?
        if (connection == null) {
            // No, just return
            return;
        }

        // Are we inside a transaction?
        if (connection.getAutoCommit() == false) {
            // No, we can close the connection
            connection.close();
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

    @Override
    public void innerDeleteAll(Descriptors.Descriptor descriptor, String protobufTypeName) throws SQLException {
        // Start building the DELETE statement
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append(DELETE_FROM);
        deleteSql.append(protobufTypeName);

        PreparedStatement preparedStatement = getNewOrExistingConnection().prepareStatement(deleteSql.toString());
        preparedStatement.execute();
    }

    @Override
    protected void innerDelete(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException {
        // Start building the DELETE statement
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append(DELETE_FROM);
        deleteSql.append(protobufTypeName);

        // Add the WHERE clause
        idWhereClause(fieldDescriptor.getName(), deleteSql);

        // Get a connection to the database and prepare the statement
        PreparedStatement preparedStatement = getNewOrExistingConnection().prepareStatement(deleteSql.toString());
        preparedStatement.setObject(1, message.getField(fieldDescriptor));
        preparedStatement.execute();
    }

    private Connection getNewOrExistingConnection() throws SQLException {
        if (currentConnection == null) {
            currentConnection = dataSource.getConnection();
        }

        return currentConnection;
    }

    @Override
    public void startTransaction() throws SQLException {
        getNewOrExistingConnection().setAutoCommit(false);
    }

    @Override
    public void rollback() throws SQLException {
        throwExceptionIfTransactionNotStarted(ROLLBACK);

        // Rollback and close the connection
        getNewOrExistingConnection().rollback();
        getNewOrExistingConnection().close();
        currentConnection = null;
    }

    @Override
    public void commit() throws SQLException {
        throwExceptionIfTransactionNotStarted(COMMIT);

        // Commit and close the connection
        getNewOrExistingConnection().commit();
        getNewOrExistingConnection().close();
        currentConnection = null;
    }

    private void throwExceptionIfTransactionNotStarted(String errorType) throws SQLException {
        if (currentConnection == null) {
            throw new UnsupportedOperationException(errorType + " attempted without a connection");
        }

        if (currentConnection.getAutoCommit() == true) {
            throw new UnsupportedOperationException(errorType + " attempted when auto-commit was on");
        }
    }
}