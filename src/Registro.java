import java.math.BigDecimal;
import java.util.UUID;


public class Registro {

	private String 		idCliente; 
	private Double		monto;
	private char 		tipoOp;
	
	
	
	public Registro(String idCliente, Double monto, char tipoOp) {
		super();
		this.idCliente = idCliente;
		this.monto = monto;
		this.tipoOp = tipoOp;
	}
	
	public String getIdCliente() {
		return idCliente;
	}
	public void setIdCliente(String idCliente) {
		this.idCliente = idCliente;
	}
	public Double getMonto() {
		return monto;
	}
	public void setMonto(Double bigDecimal) {
		this.monto = bigDecimal;
	}
	public char getTipoOp() {
		return tipoOp;
	}
	public void setTipoOp(char tipoOp) {
		this.tipoOp = tipoOp;
	}
	

}
