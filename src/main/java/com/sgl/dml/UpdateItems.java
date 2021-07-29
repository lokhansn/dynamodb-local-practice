package com.sgl.dml;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

/**
 * Updating an item in DynamoDB mainly consists of specifying the full primary key and table name for the item.
 * It requires a new value for each attribute you modify. The operation uses UpdateItem,
 * which modifies the existing items or creates them on discovery of a missing item.
 * <p>
 * In updates, you might want to track the changes by displaying the original and new values,
 * before and after the operations. UpdateItem uses the ReturnValues parameter to achieve this.
 * <p>
 * Note − The operation does not report capacity unit consumption,
 * but you can use the ReturnConsumedCapacity parameter.
 */
public class UpdateItems {

    public static void main(String[] args) {

        Table table = getDynamoDB().getTable("ProductList");
        // After running the put items run this
        updateItem(table);



    }

    private static void updateItem(Table table) {

        // In the put item value of the price of 50000
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("ID", 101, "Nomenclature", "PolyBlaster 101")
                .withUpdateExpression("set #ATTR = :VAL1")
                .withNameMap(new NameMap().with("#ATTR","Price"))
                .withValueMap(new ValueMap().withInt(":VAL1", 40000))
                .withReturnValues(ReturnValue.ALL_NEW);

        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println("Displaying updated item...");
        System.out.println(outcome.getItem().toJSONPretty());
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

/**
 Old Item:

 {
     "Availability" : true,
     "Description" : "101 Description",
     "Category" : "Hybrid Power Polymer Cutter",
     "Reviews" : {
     "5 Star" : [ "Shocking high performance.", "Unparalleled in its market." ],
     "1 Star" : [ "The worst offering in its market." ]
     },
     "Price" : 50000,
     "Nomenclature" : "PolyBlaster 101",
     "Qty" : null,
     "Images" : {
     "Lateral" : "http://xyz.com/products/101_LFTside.jpg",
     "Posterior" : "http://xyz.com/products/101_back.jpg",
     "Anterior" : "http://xyz.com/products/101_front.jpg"
     },
     "ProductCategory" : "Laser Cutter",
     "Make" : "Brand - XYZ",
     "ID" : 101,
     "Items Related" : [ 123, 456, 789 ]
 }

 Updated Item:

 {
     "Availability" : true,
     "Category" : "Hybrid Power Polymer Cutter",
     "Description" : "101 Description",
     "Reviews" : {
     "5 Star" : [ "Shocking high performance.", "Unparalleled in its market." ],
     "1 Star" : [ "The worst offering in its market." ]
     },
     "Price" : 40000,
     "Nomenclature" : "PolyBlaster 101",
     "Images" : {
     "Lateral" : "http://xyz.com/products/101_LFTside.jpg",
     "Posterior" : "http://xyz.com/products/101_back.jpg",
     "Anterior" : "http://xyz.com/products/101_front.jpg"
     },
     "Qty" : null,
     "ProductCategory" : "Laser Cutter",
     "Make" : "Brand - XYZ",
     "ID" : 101,
     "Items Related" : [ 123, 456, 789 ]
 }
 */
