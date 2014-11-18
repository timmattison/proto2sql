package com.timmattison.proto2sql.sql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by timmattison on 11/4/14.
 */
public interface ProtobufPersistence {
    /**
     * SELECTs a protobuf from a database
     *
     * @param idName  if filtering (WHERE clause) is desired this is the name of the field to filter by, NULL otherwise
     * @param id      if filtering (WHERE clause) is desired this is the value of the field to filter by, NULL otherwise
     * @param builder a builder for the type of protobuf requested
     * @return
     * @throws SQLException
     * @throws JsonFormat.ParseException
     */
    public List<Message> select(String idName, String id, Message.Builder builder) throws SQLException, JsonFormat.ParseException;

    /**
     * INSERTs a protobuf into a database
     *
     * @param message         the protobuf itself
     * @param fieldDescriptor the field to use as its ID
     * @throws SQLException
     */
    public void insert(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException;

    /**
     * UPDATEs a protobuf in a database where the ID field has not changed
     *
     * @param message         the protobuf itself
     * @param fieldDescriptor the field to use as its ID
     * @throws SQLException
     */
    public void update(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException;

    /**
     * UPDATEs a protobuf in a database where the ID field has changed
     *
     * @param message         the protobuf itself
     * @param fieldDescriptor the field to use as its ID
     * @param previousId      the ID of the object before the update
     * @throws SQLException
     */
    public void update(Message message, Descriptors.FieldDescriptor fieldDescriptor, Object previousId) throws SQLException;

    /**
     * DELETEs one protobuf of a certain type from the database
     *
     * @param message         the protobuf itself
     * @param fieldDescriptor the field to use as its ID
     * @throws SQLException
     */
    public void delete(Message message, Descriptors.FieldDescriptor fieldDescriptor) throws SQLException;

    /**
     * DELETEs all protobufs of a certain type from the database
     *
     * @param descriptor the descriptor for the protobuf
     * @throws SQLException
     */
    public void deleteAll(Descriptors.Descriptor descriptor) throws SQLException;

    /**
     * Starts a transaction
     *
     * @throws SQLException
     */
    public void startTransaction() throws SQLException;

    /**
     * Rolls a transaction back
     *
     * @throws SQLException
     */
    public void rollback() throws SQLException;

    /**
     * Commits a transaction
     *
     * @throws SQLException
     */
    public void commit() throws SQLException;
}