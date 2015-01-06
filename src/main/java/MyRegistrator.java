import java.io.Serializable;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;


public class MyRegistrator implements KryoRegistrator, Serializable{
	 
	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register( org.apache.spark.sql.cassandra.CassandraSQLRow.class);
		
	}
	}