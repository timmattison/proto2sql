package com.timmattison.proto2sql.sql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by timmattison on 11/4/14.
 */
public class InMemoryProtobufPersistence extends AbstractProtobufPersistence implements ProtobufPersistence {
    private final Map<String, List<Message>> protobufs = new HashMap<String, List<Message>>();

    @Override
    public List<Message> innerSelect(String idName, String id, Message.Builder builder, String protobufTypeName) throws SQLException, JsonFormat.ParseException {
        List<Message> allMessages = protobufs.get(protobufTypeName);

        if (allMessages == null) {
            return null;
        }

        List<Message> filteredMessages = new ArrayList<Message>();

        for (Message message : allMessages) {
            if (passesFilter(message, idName, id)) {
                filteredMessages.add(message);
            }
        }

        return filteredMessages;
    }

    private boolean passesFilter(Message message, String idName, String id) {
        // Did they specify an ID?
        if (id != null) {
            // Yes, figure out the ID field if necessary
            idName = setDefaultIdFieldNameIfNecessary(idName);
        } else {
            // No, no ID specified.  No filtering necessary.
            return true;
        }

        String idFieldValue = (String) message.getField(message.getDescriptorForType().findFieldByName(idName));

        // Return whether or not they match
        return id.equals(idFieldValue);
    }

    @Override
    protected void innerInsert(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException {
        List<Message> list = getList(protobufTypeName);

        list.add(message);
    }

    private List<Message> getList(String protobufTypeName) {
        if (!protobufs.containsKey(protobufTypeName)) {
            protobufs.put(protobufTypeName, new ArrayList<Message>());
        }

        return protobufs.get(protobufTypeName);
    }

    private void setList(String protobufTypeName, List<Message> newList) {
        protobufs.put(protobufTypeName, newList);
    }

    @Override
    protected void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) throws SQLException {
        List<Message> existingList = getList(protobufTypeName);

        Object id = message.getField(fieldDescriptor);

        List<Message> newList = new ArrayList<Message>();

        for (Message existingMessage : existingList) {
            if (existingMessage.getField(fieldDescriptor).equals(id)) {
                newList.add(message);
            } else {
                newList.add(existingMessage);
            }
        }

        setList(protobufTypeName, newList);
    }

    @Override
    public void innerDeleteAll(Descriptors.Descriptor descriptor, String protobufTypeName) throws SQLException {
        protobufs.put(protobufTypeName, new ArrayList<Message>());
    }

    @Override
    protected void innerDelete(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName) {
        List<Message> existingList = getList(protobufTypeName);

        Object id = message.getField(fieldDescriptor);

        List<Message> newList = new ArrayList<Message>();

        for (Message existingMessage : existingList) {
            if (!existingMessage.getField(fieldDescriptor).equals(id)) {
                newList.add(existingMessage);
            }
        }

        setList(protobufTypeName, newList);
    }

    @Override
    public void innerUpdate(Message message, Descriptors.FieldDescriptor fieldDescriptor, String protobufTypeName, Object previousId) throws SQLException {
        if (previousId == null) {
            update(message, fieldDescriptor);
            return;
        }

        List<Message> existingList = getList(protobufTypeName);

        List<Message> newList = new ArrayList<Message>();

        for (Message existingMessage : existingList) {
            if (!existingMessage.getField(fieldDescriptor).equals(previousId)) {
                newList.add(existingMessage);
            } else {
                newList.add(message);
            }
        }

        setList(protobufTypeName, newList);
    }

    @Override
    public void startTransaction() throws SQLException {
        // Do nothing
    }

    @Override
    public void rollback() throws SQLException {
        // Do nothing
    }

    @Override
    public void commit() throws SQLException {
        // Do nothing
    }

    @Override
    public void resultSetToProtobuf(Message.Builder builder, ResultSet resultSet) throws SQLException, JsonFormat.ParseException {
        throw new UnsupportedOperationException("This method is not relevant in the in-memory implementation");
    }
}