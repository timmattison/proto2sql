package com.timmattison.proto2sql.sql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by timmattison on 11/14/14.
 */
public abstract class AbstractProtobufPersistence implements ProtobufPersistence {
    protected static final String DEFAULT_ID_NAME = "id";

    @Override
    public void delete(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException {
        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Do the actual delete
        innerDelete(message, fieldDescriptor, protobufTypeName);
    }

    @Override
    public final void deleteAll(Descriptors.Descriptor descriptor) throws SQLException {
        String protobufTypeName = getTableName(descriptor);

        // Do the actual delete
        innerDeleteAll(descriptor, protobufTypeName);
    }

    protected static String setDefaultIdFieldNameIfNecessary(String idName) {
        // Did they specify the ID name?
        if (idName == null) {
            // No, use the default ID name
            idName = DEFAULT_ID_NAME;
        }

        return idName;
    }

    @Override
    public final List<Message> select(String idName, String id, Message.Builder builder) throws SQLException, JsonFormat.ParseException {
        // Get the name of the table that this protobuf resides in in the database
        String tableName = getTableName(builder.getDescriptorForType());

        if (id != null) {
            idName = setDefaultIdFieldNameIfNecessary(idName);
        }

        // Do the actual select
        return innerSelect(idName, id, builder, tableName);
    }

    @Override
    public void update(Message message, Descriptors.FieldDescriptor fieldDescriptor, Object previousId) throws SQLException {
        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Do the actual update
        innerUpdate(message, fieldDescriptor, protobufTypeName, previousId);
    }

    @Override
    public final void insert(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException {
        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Do the actual insert
        innerInsert(message, fieldDescriptor, protobufTypeName);
    }

    @Override
    public final void update(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException {
        // Get the descriptor
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        // Get the type name
        String protobufTypeName = getTableName(descriptor);

        // Do the actual update
        innerUpdate(message, fieldDescriptor, protobufTypeName);
    }

    private String getDefaultIdIfNecessary(Message message, String id) {
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
        return id;
    }

    protected String getTableName(Descriptors.Descriptor descriptor) {
        // Get the table name by removing "domain." from the full name and then by replacing all dots with underscores
        return descriptor.getFullName().replaceFirst("domain.", "").replaceAll("\\.", "_");
    }

    protected abstract List<Message> innerSelect(String idName, String id, Message.Builder builder, String tableName) throws SQLException, JsonFormat.ParseException;

    protected abstract void innerInsert(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException;

    protected abstract void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException;

    protected abstract void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName, Object previousId) throws SQLException;

    protected abstract void innerDeleteAll(Descriptors.Descriptor descriptor, String protobufTypeName) throws SQLException;

    protected abstract void innerDelete(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException;

}
