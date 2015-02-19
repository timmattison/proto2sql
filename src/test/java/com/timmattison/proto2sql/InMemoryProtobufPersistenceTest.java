package com.timmattison.proto2sql;

import com.timmattison.proto2sql.sql.InMemoryProtobufPersistence;

import java.util.Random;

/**
 * Created by timmattison on 11/18/14.
 */
public class InMemoryProtobufPersistenceTest extends ProtobufPersistenceTest {
    @Override
    protected void innerTeardown() {
        // Do nothing
    }

    @Override
    protected void innerSetup() {
        protobufPersistence = new InMemoryProtobufPersistence();
        random = new Random(0);
    }
}
