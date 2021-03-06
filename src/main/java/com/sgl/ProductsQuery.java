package com.sgl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import java.util.Iterator;

public class ProductsQuery {

    public static void main(String[] args) {
        DynamoDB dynamoDB = getDynamoDB();

        Table products = dynamoDB.getTable("Products");

//        HashMap<String, String> nameMap = new HashMap<>();
//        HashMap<String, Object> valueMap = new HashMap<>();
//        nameMap.put("#ID", "ID");
//        valueMap.put(":xxx", 122);

//        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#ID = :xxx")
//                .withNameMap(nameMap)
//                .withValueMap(valueMap);

        findProductsById(products, 122);
        findProductsById(products, 120);

    }

    private static void findProductsById(Table products, int productId) {
        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#ID = :xxx")
                .withNameMap(new NameMap().with("#ID", "ID"))
                .withValueMap(new ValueMap().with(":xxx", productId));

        try {
            System.out.println("Product with the ID "+productId);
            ItemCollection<QueryOutcome> items = products.query(querySpec);
            Iterator<Item> iterator = items.iterator();

            while (iterator.hasNext()) {
                Item item = iterator.next();
                System.out.println(item.getNumber("ID") + ": " + item.getString("Nomenclature"));
            }

        } catch (Exception e) {
            System.err.println("Cannot find products with the ID number "+productId);
            System.err.println(e.getMessage());
        }
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
