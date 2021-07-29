package com.sgl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;


public class ProductsLoadData {


    public static void main(String[] args) throws IOException, URISyntaxException {
        DynamoDB dynamoDB = getDynamoDB();
        Table table = dynamoDB.getTable("Products");
        JsonParser parser = new JsonFactory().createParser(getFileFromResource("productinfo.json"));

        JsonNode rootNode = new ObjectMapper().readTree(parser);

        Iterator<JsonNode> iterator = rootNode.iterator();
        ObjectNode currentNode;

        while (iterator.hasNext()) {
            currentNode = (ObjectNode) iterator.next();
            int id = currentNode.path("ID").asInt();
            String nomenclature = currentNode.path("Nomenclature").asText();

            try {
                table.putItem(new Item()
                        .withPrimaryKey("ID", id, "Nomenclature", nomenclature)
                        .withJSON("Stat", currentNode.path("Stat").toString()));
                System.out.println("Successful load: " + id + " " + nomenclature);
            } catch (Exception e) {
                System.err.println("Cannot add product: " + id + " " + nomenclature);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();

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

    private static File getFileFromResource(String fileName) throws URISyntaxException {
        ClassLoader classLoader = ProductsLoadData.class.getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("File Not Found! " + fileName);
        } else {
            return new File(resource.toURI());
        }
    }
}
