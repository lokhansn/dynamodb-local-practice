package com.sgl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import java.util.Iterator;

public class ProductScan {

    public static void main(String[] args) {
        DynamoDB dynamoDB = getDynamoDB();

        Table productsTable = dynamoDB.getTable("Products");

        ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression("#ID, Nomenclature, Stat.sales")
                .withFilterExpression("#ID between :start_id and :end_id")
                .withNameMap(new NameMap().with("#ID", "ID"))
                .withValueMap(new ValueMap().
                        withNumber(":start_id", 110).
                        withNumber(":end_id", 130));


        try{

            ItemCollection<ScanOutcome> products = productsTable.scan(scanSpec);
            Iterator<Item> iterator = products.iterator();
            while(iterator.hasNext()){
                Item item = iterator.next();
                System.out.println(item);
            }

        }catch (Exception e){
            System.err.println("Cannot perform a table scan:");
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
