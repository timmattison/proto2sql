package com.timmattison.proto2sql.sql;

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
     * @param message the protobuf itself
     * @param idName  the field to use as its ID
     * @param id      the value of its ID
     * @throws SQLException
     */
    public void insert(Message message, String idName, String id) throws SQLException;

    /**
     * UPDATEs a protobuf in a database
     *
     * @param message the protobuf itself
     * @param idName  the field to use as its ID
     * @param id      the value of its ID
     * @throws SQLException
     */
    public void update(Message message, String idName, String id) throws SQLException;
}