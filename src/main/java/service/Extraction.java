package service;

import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import org.eclipse.microprofile.config.ConfigProvider;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class Extraction {

    public void start(String sql) {
        
        //change this to read env variables
        PgConnectOptions connectOptions = new PgConnectOptions().setPort(5432).setHost("localhost").setDatabase("luizinho_warehouse").setUser("admin").setPassword("admin102030");
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
        SqlClient client = PgPool.client(connectOptions, poolOptions);
        
        client.query(sql).execute().onComplete(ar -> {
            if (ar.succeeded()) {
                RowSet<Row> rows = ar.result();
                System.out.println("Got " + rows.size() + " rows ");

                try{
                    writeToFile(rows);
                }catch (IOException ioe){
                    System.out.println("Failure: " + ioe.getMessage());
                }

            } else {
                System.out.println("Failure: " + ar.cause().getMessage());
            }

            client.close();
        });
    }



    private void writeToFile(RowSet<Row> rows) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter("file-"+new Date().getTime()+".txt", true));
        writer.append("HEADER"+System.lineSeparator());

        for (Row row : rows) {
            System.out.println("Register found " + row.getLong(0) + " " + row.getString(1));

            writer.append(String.valueOf(row.getLong(0)));
            writer.append(addSpaces("file.fields.param0.size"));
            writer.append(row.getString(1));
            writer.append(addSpaces("file.fields.param1.size"));

        }

        writer.append(System.lineSeparator()+"FOOTER");
        writer.close();
    }



    private String addSpaces(String fieldConfig){
        int size = ConfigProvider.getConfig().getValue(fieldConfig, Integer.class);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < size; i++){
            sb.append(" ");
        }
        return sb.toString();
    }

}
