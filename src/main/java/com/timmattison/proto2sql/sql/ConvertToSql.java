package com.timmattison.proto2sql.sql;

import com.google.protobuf.Message;

import java.util.List;

/**
 * Created by timmattison on 10/22/14.
 */
public interface ConvertToSql {
    /**
     * Some Protobuf datatypes
     */
    public static final String STRING = "STRING";
    public static final String INT64 = "INT64";
    public static final String MESSAGE = "MESSAGE";
    public static final String ENUM = "ENUM";
    public static final String BOOL = "BOOL";

    /**
     * Generate a list of SQL statements that create the table definitions for this Protobuf
     *
     * @param message
     * @return
     */
    public List<String> generateSql(Message message);
}
