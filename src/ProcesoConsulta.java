import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;




public class ProcesoConsulta {

	private static final String SQL_SELECT = "select id_cliente, sum(case tipo_operacion when 'C' then monto when 'D' then (monto * (-1)) end) as balance from reg1 group by id_cliente";
	private static Statement stmt;

	

	static void consulta(Connection conn) throws SQLException {
		
		try {
	 
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(SQL_SELECT);
			while (rs.next()) {
				String id = rs.getString("ID_CLIENTE");
				float monto = rs.getFloat("BALANCE");
		
			}
		} finally {
			if (stmt != null) { stmt.close(); }
		}
		}

}




