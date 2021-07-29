package com.sgl.dml;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.*;

public class PutItems {
    final static DynamoDB dynamoDB = getDynamoDB();


    public static void main(String[] args) {
        createItems("ProductList");

    }

    private static void createItems(String tableName) {
        Table table = dynamoDB.getTable(tableName);

        try {
            Item item = createLargerItem();
            table.putItem(item);
            Item item1 = new Item()
                    .withPrimaryKey("ID", 303)
                    .withString("Nomenclature", "Polymer Blaster 4000")
                    .withStringSet("Manufacturers", new HashSet<String>(Arrays.asList("XYZ inc", "PQR corp")))
                    .withNumber("Price", 50000)
                    .withBoolean("InProduction", true)
                    .withString("Category", "Laser Cutter");

            Item item2 = new Item()
                    .withPrimaryKey("ID", 313)
                    .withString("Nomenclature", "Agitatatron 2000")
                    .withStringSet("Manufacturers", new HashSet<String>(Arrays.asList("XYZ inc", "CDE corp")))
                    .withNumber("Price", 40000)
                    .withBoolean("InProduction", true)
                    .withString("Category", "Agitator");
            table.putItem(item1);
            table.putItem(item2);

        } catch (Exception e) {
            System.err.println("Cannot create items.");
            System.err.println(e.getMessage());
        }

    }

    private static Item createLargerItem() {
        List<Integer> relItems = Arrays.asList(123, 456, 789);
        Map<String, String> photos = new HashMap<>() {
            {
                put("Anterior", "http://xyz.com/products/101_front.jpg");
                put("Posterior", "http://xyz.com/products/101_back.jpg");
                put("Lateral", "http://xyz.com/products/101_LFTside.jpg");
            }
        };
        Map<String, List<String>> prodReviews = new HashMap<>() {
            {
                List<String> fiveStarReview = Arrays.
                        asList("Shocking high performance.", "Unparalleled in its market.");
                put("5 Star", fiveStarReview);
                List<String> oneStarReview = Arrays.asList("The worst offering in its market.");
                put("1 Star", oneStarReview);

            }
        };

        Item item = new Item()
                .withPrimaryKey("ID", 101)
                .withString("Nomenclature", "PolyBlaster 101")
                .withString("Description", "101 Description")
                .withString("Category", "Hybrid Power Polymer Cutter")
                .withString("Make", "Brand - XYZ")
                .withNumber("Price", 50000)
                .withString("ProductCategory", "Laser Cutter")
                .withBoolean("Availability", true)
                .withNull("Qty")
                .withList("Items Related", relItems)
                .withMap("Images", photos)
                .withMap("Reviews", prodReviews);
        return item;
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
