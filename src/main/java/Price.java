import java.io.Serializable;


public class Price implements Serializable{

	@Override
	public String toString() {
		return "Price [partnumber=" + partnumber + ", offerprice=" + offerprice + "]";
	}
	private String partnumber;
	private double offerprice;
	public String getPartnumber() {
		return partnumber;
	}
	public void setPartnumber(String partnumber) {
		this.partnumber = partnumber;
	}
	public double getOfferprice() {
		return offerprice;
	}
	public void setOfferprice(double offerprice) {
		this.offerprice = offerprice;
	}
	
}
