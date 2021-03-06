package com.timmattison.proto2sql.sql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by timmattison on 10/22/14.
 */
public class ConvertToPostgresql implements ConvertToSql {
    private static final String VARCHAR_255 = "varchar";
    private static final String TEXT = "text";
    private static final String BIGINT = "bigint";
    private static final String INTEGER = "integer";
    private static final String NOT_NULL = "not null";
    private static final String SQL_ARRAY = "[]";
    private static final String BOOLEAN = "boolean";

    private static final String PRIMARY_KEY = "primary key";
    private static final String ID = "id";

    @Override
    public List<String> generateSql(Message message) {
        // Create an output array list and an array list for our enums
        List<String> output = new ArrayList<String>();
        List<String> enums = new ArrayList<String>();

        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = descriptor.getName();

        // Loop through all of the fields
        StringBuilder stringBuilder = innerGenerateSql(enums, descriptor, protobufTypeName);

        // Did we create any enums?
        if (enums.size() != 0) {
            // Yes, add them to the output before the table definition
            output.addAll(enums);
        }

        // Did we build a table definition?
        if (stringBuilder.length() != 0) {
            // Yes, close it and add it to the output
            stringBuilder.append("\n);");

            output.add(stringBuilder.toString());
        }

        return output;
    }

    private StringBuilder innerGenerateSql(List<String> enums, Descriptors.Descriptor descriptor, String protobufTypeName) {
        StringBuilder stringBuilder = new StringBuilder();
        String separator = "";

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            // We don't know the SQL type yet
            String sqlType = null;

            // Get the name and type name of the current field
            String name = field.getName();
            String typeName = field.getType().name();

            if (STRING.equals(typeName)) {
                // String type, convert it to varchar 255
                sqlType = VARCHAR_255;
            } else if (INT64.equals(typeName)) {
                // Int64 type, convert it to a bigint
                sqlType = BIGINT;
            } else if (INT32.equals(typeName)) {
                // Int32 type, convert it to a bigint
                sqlType = INTEGER;
            } else if (BOOL.equals(typeName)) {
                // Bool type, convert it to a boolean
                sqlType = BOOLEAN;
            } else if (MESSAGE.equals(typeName)) {
                // Treat this as text
                sqlType = TEXT;
            } else if (ENUM.equals(typeName)) {
                // Enum type, create the SQL for the enum
                enums.add(createEnum(field));

                // Set the SQL type to the new enum name
                sqlType = getEnumTypeName(field);
            } else {
                // Don't know how to handle this.  Fail.
                throw new UnsupportedOperationException("Can't find type " + typeName);
            }

            // Did we already create the CREATE TABLE portion of this statement?
            if (stringBuilder.length() == 0) {
                // No, create it now
                stringBuilder.append("create table ");

                stringBuilder.append(protobufTypeName);
                stringBuilder.append(" (");
            }

            // Add the separator, name, and type
            stringBuilder.append(separator);
            stringBuilder.append("\n  \"");
            stringBuilder.append(name);
            stringBuilder.append("\" ");
            stringBuilder.append(sqlType);

            // Is this field repeated?
            if (field.isRepeated()) {
                // Yes, make it a SQL array
                stringBuilder.append(SQL_ARRAY);
            }

            // Spacing
            stringBuilder.append(" ");

            // Is this field required?
            if (field.isRequired()) {
                // Yes, is it the ID field?
                if (ID.equals(name)) {
                    stringBuilder.append(PRIMARY_KEY);
                } else {
                    // No, make it NOT NULL
                    stringBuilder.append(NOT_NULL);
                }
            }

            // Update the separator for the next field
            separator = ",";
        }

        return stringBuilder;
    }

    private static String createEnum(Descriptors.FieldDescriptor field) {
        // Get the type name of the enum
        String enumTypeName = getEnumTypeName(field);

        // Create a string builder and start building the enum type
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("create type ");
        stringBuilder.append(enumTypeName);
        stringBuilder.append(" as enum(");

        // Start with a blank separator
        String separator = "";

        // Loop through each possible enum value
        for (Descriptors.EnumValueDescriptor value : field.getEnumType().getValues()) {
            // Add the enum value
            stringBuilder.append(separator);
            stringBuilder.append("'");
            stringBuilder.append(value.getName());
            stringBuilder.append("'");

            // Update the separator for the next enum value
            separator = ", ";
        }

        // Close out the enum
        stringBuilder.append(");");

        // Return the final string
        return stringBuilder.toString();
    }

    private static String getEnumTypeName(Descriptors.FieldDescriptor field) {
        return field.getEnumType().getName();
    }
}
