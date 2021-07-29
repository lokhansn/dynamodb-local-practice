package com.sgl.dml;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class RetrieveItem {

    final static DynamoDB dynamoDB = getDynamoDB();


    public static void main(String[] args) {
        // Before Executing this execute src/main/java/com/sgl/dml/RetrieveItem.java
        // and src/main/java/com/sgl/ProductsCreateTable.java
        retrieveItems("ProductList");

    }

    private static void retrieveItems(String tableName) {
        Table table = dynamoDB.getTable(tableName);

        // You must specify hash key and range key of the table if both are present while creating the table
        // if only hash key is used while creating the table then there is no need to pass the second argument (range key)
        // with projection only the attributes declared after the hash key and range key will be fetched from the table for the perticular item
        Item item1 = table.getItem("ID", 303,
                "Nomenclature", "Polymer Blaster 4000",
                "ID, Nomenclature, Manufacturers", null);
        System.out.println(item1.toJSONPretty());

        Item item2 = table.getItem("ID", 101,
                "Nomenclature", "PolyBlaster 101");
        System.out.println(item2.toJSONPretty());
    }

    private static DynamoDB getDynamoDB() {
        final String amazonDynamoDBEndpoint = "http://localhost:8000";
        final String amazonAWSAccessKey = "anyaccesskey";
        final String amazonAWSSecretKey = "anysecretkey";
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(amazonDynamoDBEndpoint, "ap-south-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(amazonAWSAccessKey, amazonAWSSecretKey)))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        return dynamoDB;
    }
}
