package ecnu.db.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class DataInputFromFile {
    private FileReader fileReader;
    private CSVParser csvParser;
    public DataInputFromFile(int tableIndex){
        try {
            fileReader = new FileReader("randomData/t" + tableIndex);
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withSkipHeaderRecord();
            csvParser = new CSVParser(fileReader, csvFileFormat);
        } catch (Exception e) {
            System.out.println("读取文件randomData/t" + tableIndex + "失败");
        }
    }

    public double[][] readData() throws IOException {
        List<CSVRecord> csvRecords = csvParser.getRecords();
        int lineNum=csvRecords.size();
        int colNum=csvRecords.get(0).size();
        double[][] results=new double[lineNum][colNum-1];
        int i=0;
        for(CSVRecord csvRecord:csvRecords){
            for(int j=0;j<colNum-1;j++){
                results[i][j]=Double.valueOf(csvRecord.get(j+1));
            }
            i++;
        }
        try {
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    public static void  main(String[] args) throws IOException {
        DataInputFromFile dataInputFromFile=new DataInputFromFile(0);
        dataInputFromFile.readData();
    }
}
