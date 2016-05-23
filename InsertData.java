import java.io.IOException;

import java.io.FileReader;
import com.opencsv.CSVReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData{

	public static void main(String[] args) throws IOException
	  {
		//CREA LA TABLA SI NO EXISTE
		try{
			HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
		    HTableDescriptor htable = new HTableDescriptor("practica5"); 
		    htable.addFamily( new HColumnDescriptor("user"));
		    htable.addFamily( new HColumnDescriptor("datos"));
		    System.out.println( "Connecting..." );
		    HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
		    System.out.println( "Creating Table..." );
		    hbase_admin.createTable( htable );
		    System.out.println("Done!");
		} catch(TableExistsException e){
			System.out.println("Ya existe la tabla practica5.");
		}
		
	  //CONECTA CON LA TABLA
	  org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
	  HTable table = new HTable(config, "practica5");
	  
	  //LEE EL FICHERO
	  try {
	      //csv file containing data
	      String strFile = "data.csv";
	      CSVReader reader = new CSVReader(new FileReader(strFile));
	      String [] nextLine;
	      int lineNumber = 0;
	      //recorre cada linea
	      while ((nextLine = reader.readNext()) != null) {
	    	  
	        lineNumber++;
	        Put p = new Put(Bytes.toBytes("row"+lineNumber));
	        System.out.println("Line # " + lineNumber);
	        
	        p.add(Bytes.toBytes("user"), Bytes.toBytes("uid"),Bytes.toBytes(nextLine[0])); //código unico de usuario
	  	  	p.add(Bytes.toBytes("user"),Bytes.toBytes("grupo"),Bytes.toBytes(nextLine[1])); //grupo A o B
	        
	  	  	int aux;
	        // nextLine[] is an array of values from the line
	  	  	// inserta cada característica
	        for (int i=2; i<nextLine.length; i++) {
	        	aux = i - 1;
	        	p.add(Bytes.toBytes("datos"), Bytes.toBytes("caract"+aux),Bytes.toBytes(nextLine[i]));
	        }
	        table.put(p);
	      }
	      
	    } catch(Exception e) {    	
	    	e.printStackTrace();
	    }
	  
	 
	  //LEER DATOS
	  
	  /*Get g = new Get(Bytes.toBytes("row1"));
	  Result r = table.get(g);

	  byte [] value = r.getValue(Bytes.toBytes("user"),Bytes.toBytes("uid"));
	  byte [] value1 = r.getValue(Bytes.toBytes("user"),Bytes.toBytes("grupo"));

	  String valueStr = Bytes.toString(value);
	  String valueStr1 = Bytes.toString(value1);
	  System.out.println("GET: " +"user: "+ valueStr+"datos: "+valueStr1);
	  Scan s = new Scan();
	  s.addColumn(Bytes.toBytes("user"), Bytes.toBytes("uid"));
	  s.addColumn(Bytes.toBytes("user"), Bytes.toBytes("grupo"));
	  ResultScanner scanner = table.getScanner(s);

	  try
	  {
	     for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
	     {
	        System.out.println("Found row : " + rnext);
	     }
	  } finally
	    {
	       scanner.close();
	    }  
*/	  }
}