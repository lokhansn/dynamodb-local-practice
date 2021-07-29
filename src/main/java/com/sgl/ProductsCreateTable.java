package com.sgl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.io.IOException;
import java.util.Arrays;

public class ProductsCreateTable {

    private static final String amazonDynamoDBEndpoint = "http://localhost:8000";
    private static final String amazonAWSAccessKey = "anyaccesskey";
    private static final String amazonAWSSecretKey = "anysecretkey";
    static AmazonDynamoDB client;

    public static void main(String[] args) throws IOException {

        client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(amazonDynamoDBEndpoint, "ap-south-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(amazonAWSAccessKey, amazonAWSSecretKey)))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Products";
        try {
            System.out.println("Creating the table, wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("ID", KeyType.HASH), // the partition key
                            // the sort key
                            new KeySchemaElement("Nomenclature", KeyType.RANGE)
                    ),
                    Arrays.asList(
                            new AttributeDefinition("ID", ScalarAttributeType.N),
                            new AttributeDefinition("Nomenclature", ScalarAttributeType.S)
                    ),
                    new ProvisionedThroughput(10L, 10L)
            );
            table.waitForActive();
            System.out.println("Table created successfully.  Status: " +
                    table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Cannot create the table: ");
            System.err.println(e.getMessage());
        }
    }
}
