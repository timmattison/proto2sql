package com.timmattison.proto2sql.sql;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by timmattison on 11/4/14.
 */
public interface ProtobufPersistence {
    public List<Message> select(String idName, String id, Message.Builder builder) throws SQLException, JsonFormat.ParseException;

    public void insert(Message message, String idName, String id) throws SQLException;

    public void update(Message message, String idName, String id) throws SQLException;
}