import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Scanner;
import java.util.UUID;



public class ProcesoFile {
	
	

	public static void cargarTabla(Connection conn) throws Exception {
		
		
		 int i = 0;
		 
		 String sql = "INSERT INTO REG4(id_cliente, monto, tipo_operacion) VALUES (?, ?, ?)";

		 
		 	final int batchSize = 10000;
		 	int count = 0;
		 	

		 	FileInputStream inputStream = null;
			Scanner sc = null;
			try {
			    inputStream = new FileInputStream("C:\\Users\\Yo\\Desktop\\movimientos_1000000.txt");
			    sc = new Scanner(inputStream, "UTF-8");
			    
			    PreparedStatement ps = conn.prepareStatement(sql);
			   
			    while (sc.hasNextLine()) {
			        
			    	String line = sc.nextLine();
			        
			    	String[] partes = line.split(",");

				     String id = partes[0];
				     
				    
				     Double monto = Double.parseDouble(partes[1]);
				     
				     String c = partes[2];
				     
				     ps.setString(1, id);
				 	 ps.setDouble(2, monto);
				 	 ps.setString(3, c);
				 	 
				 	 ps.addBatch();
				     
				    
				     
				     if(++count % batchSize == 0) {
				    	 System.out.println("count:" + count);
				 	        ps.executeBatch();
				 	    }
				 	
			    }
				 ps.executeBatch(); 
				 ps.close();
				 
			    
			    
			    if (sc.ioException() != null) {
			        throw sc.ioException();
			    }
			} finally {
			    if (inputStream != null) {
			        inputStream.close();
			    }
			    if (sc != null) {
			        sc.close();
			    }
			    
			 
			}
		 	
	
	}
}
