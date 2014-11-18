package com.timmattison.proto2sql;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.timmattison.proto2sql.sql.InMemoryProtobufPersistence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by timmattison on 11/14/14.
 */
public class TestInMemoryProtobufPersistence {
    private InMemoryProtobufPersistence inMemoryProtobufPersistence;
    private Random random;
    private int insertCount = 100;

    @Before
    public void setup() {
        inMemoryProtobufPersistence = new InMemoryProtobufPersistence();
        random = new Random(0);
    }

    @Test
    public void testInsertThenSelect() throws SQLException, JsonFormat.ParseException {
        TestProtobufs.SearchRequest searchRequest = createSearchRequest();
        inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        originalMessagePresent(searchRequest, results);
    }

    @Test
    public void testInsertManyAndCheckCount() throws SQLException, JsonFormat.ParseException {
        for (int loop = 0; loop < insertCount; loop++) {
            TestProtobufs.SearchRequest searchRequest = createRandomSearchRequest();
            inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
        }

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        Assert.assertNotNull(results);
        Assert.assertEquals(insertCount, results.size());
    }

    @Test
    public void testInsertManyAndDeleteOneByOne() throws SQLException, JsonFormat.ParseException {
        List<Message> messages = new ArrayList<Message>();
        for (int loop = 0; loop < insertCount; loop++) {
            TestProtobufs.SearchRequest searchRequest = createRandomSearchRequest();
            inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
            messages.add(searchRequest);
        }

        int count = insertCount;
        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        Assert.assertNotNull(results);
        Assert.assertEquals(count, results.size());

        for (Message message : messages) {
            inMemoryProtobufPersistence.delete(message, message.getDescriptorForType().findFieldByNumber(1));
            count--;

            // Make sure this message is no longer present
            messageNotPresent(message, inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder()));

            results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
            Assert.assertNotNull(results);
            Assert.assertEquals(count, results.size());
        }

        results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        Assert.assertNotNull(results);
        Assert.assertEquals(count, results.size());
    }

    @Test
    public void testInsertManyDeleteAllAndCheckCount() throws SQLException, JsonFormat.ParseException {
        for (int loop = 0; loop < insertCount; loop++) {
            TestProtobufs.SearchRequest searchRequest = createRandomSearchRequest();
            inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
        }

        inMemoryProtobufPersistence.deleteAll(TestProtobufs.SearchRequest.getDescriptor());

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        noMessagesPresent(results);
    }

    @Test
    public void testInsertThenDeleteAll() throws SQLException, JsonFormat.ParseException {
        TestProtobufs.SearchRequest searchRequest = createSearchRequest();
        inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
        inMemoryProtobufPersistence.deleteAll(searchRequest.getDescriptorForType());

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        noMessagesPresent(results);
    }

    private void noMessagesPresent(List<Message> results) {
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testInsertThenDeleteInserted() throws SQLException, JsonFormat.ParseException {
        TestProtobufs.SearchRequest searchRequest = createSearchRequest();
        inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
        inMemoryProtobufPersistence.delete(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        noMessagesPresent(results);
    }

    @Test
    public void testInsertThenDeleteInvalid() throws SQLException, JsonFormat.ParseException {
        TestProtobufs.SearchRequest searchRequest = createSearchRequest();
        TestProtobufs.SearchRequest searchRequest2 = createRandomSearchRequest();
        inMemoryProtobufPersistence.insert(searchRequest, searchRequest.getDescriptorForType().findFieldByNumber(1));
        inMemoryProtobufPersistence.delete(searchRequest2, searchRequest.getDescriptorForType().findFieldByNumber(1));

        List<Message> results = inMemoryProtobufPersistence.select(null, null, TestProtobufs.SearchRequest.newBuilder());
        originalMessagePresent(searchRequest, results);
    }

    private void originalMessagePresent(TestProtobufs.SearchRequest searchRequest, List<Message> results) {
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(searchRequest, results.get(0));
    }

    private void messagePresent(Message message, List<Message> messages) {
        Assert.assertNotNull(messages);

        for (Message messageInSet : messages) {
            if (messageInSet.equals(message)) {
                return;
            }
        }

        Assert.fail("Not found");
    }

    private void messageNotPresent(Message message, List<Message> messages) {
        Assert.assertNotNull(messages);

        for (Message messageInSet : messages) {
            if (messageInSet.equals(message)) {
                Assert.fail("Found");
            }
        }
    }

    private TestProtobufs.SearchRequest createSearchRequest() {
        TestProtobufs.SearchRequest.Builder builder = TestProtobufs.SearchRequest.newBuilder();

        builder.setPageNumber(1);
        builder.setQuery("Test query");
        builder.setResultPerPage(100);

        return builder.build();
    }

    private TestProtobufs.SearchRequest createRandomSearchRequest() {
        TestProtobufs.SearchRequest.Builder builder = TestProtobufs.SearchRequest.newBuilder();

        builder.setPageNumber(random.nextInt());
        builder.setQuery(new BigInteger(130, random).toString(32));
        builder.setResultPerPage(random.nextInt());

        return builder.build();
    }
}
