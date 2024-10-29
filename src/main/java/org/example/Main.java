package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Main {
    static public  void main(String[] args) {
    String csvFilePath = "src/main/resources/data.csv";

        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","PHW#84#jeor");

            String sql = "SELECT * FROM ratings"; // Query to extract table data

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            // Write CSV file
            FileWriter csvWriter = new FileWriter(csvFilePath);

            // Get column names and write as header row in CSV
            int columnCount = resultSet.getMetaData().getColumnCount();

            // Write data rows
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    csvWriter.append(resultSet.getString(i));
                    if (i < columnCount) csvWriter.append(","); // Add comma if not the last column
                }
                csvWriter.append("\n"); // New line after each row
            }

            csvWriter.flush();
            csvWriter.close();
            System.out.println("CSV file created successfully: " + csvFilePath);

            WordCount.RunJob("src/main/resources/data.csv","/test/output","/test/input/data.csv");

        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
