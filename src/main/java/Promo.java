import java.io.Serializable;


public class Promo implements Serializable{

	private String partnumber;
	private String promotionid;
	public String getPartnumber() {
		return partnumber;
	}
	public void setPartnumber(String partnumber) {
		this.partnumber = partnumber;
	}
	public String getPromotionid() {
		return promotionid;
	}
	public void setPromotionid(String promotionid) {
		this.promotionid = promotionid;
	}
	@Override
	public String toString() {
		return "Promo [partnumber=" + partnumber + ", promotionid=" + promotionid + "]";
	}
}
