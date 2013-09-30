

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.Before;
import org.junit.Test;

import com.androidyou.User;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class testhabsae {

	@Test
	public void testGet() throws IOException {
		Get get = new Get(Bytes.toBytes("001"));
		org.apache.hadoop.hbase.client.Result re = table.get(get);

		Assert.isTrue(re.containsColumn(f, q));
	}

	HTable table;
	private Configuration conf;

	byte[] f = Bytes.toBytes("c1");
	byte[] q = Bytes.toBytes("address");
	byte[] q2 = Bytes.toBytes("now");
	byte[] q3 = Bytes.toBytes("avro");

	@Before
	public void MakeSureTableIsThere() throws IOException {
		conf = new Configuration();
		table = new HTable(conf, "today");

	}

	@Test
	public void TestScan() throws IOException {
		Scan scan = new Scan();
		// scan.setStartRow(Bytes.toBytes("001"));
		// scan.setStopRow(Bytes.toBytes("004"));

		ResultScanner result = table.getScanner(scan);
		for (Result s : result) {
			System.out.println('\n' + Bytes.toString(s.getRow()));
			System.out.print(Bytes.toString(s.getValue(f, q)));
		}
		Assert.isTrue(result != null);
	}

	@Test
	public void TestPut() throws IOException {
		List<Put> lists = new ArrayList<Put>();

		Put put = new Put(Bytes.toBytes("lastupdate"));
		put.add(f, q, Bytes.toBytes(new Date().toGMTString()));

		lists.add(put);
		put = new Put(Bytes.toBytes("lastupdate"));
		put.add(f, q2, Bytes.toBytes(new Date().toGMTString()));
		lists.add(put);
		table.put(lists);
	}

	@Test
	public void TestAvroSerialization() throws IOException {
		User u = User.newBuilder().setFavoriteColor("Yellow").setName("David")
				.setFavoriteNumber(88).build();

		DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dwriter = new DataFileWriter<User>(writer);

		dwriter.create(u.getSchema(), new File("a.avo"));
		dwriter.append(u);
		u = User.newBuilder().setFavoriteColor("RED").setName("David")
				.setFavoriteNumber(8998).build();
		dwriter.append(u);
		dwriter.close();

	}

	@Test
	public void TestPutAvroSerializationToHbase() throws IOException {
		User u = User.newBuilder().setFavoriteColor("Yellow").setName("David")
				.setFavoriteNumber(88).build();

		
		DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dwriter = new DataFileWriter<User>(writer);
		ByteArrayOutputStream outs=new ByteArrayOutputStream();
		dwriter.create(u.getSchema(),outs);
		dwriter.append(u);
		u = User.newBuilder().setFavoriteColor("RED").setName("David")
				.setFavoriteNumber(8998).build();
		dwriter.append(u);
		dwriter.close();
		
		Put put=new Put(Bytes.toBytes("avrokey"));
		put.add(f, q3, outs.toByteArray());
		table.put(put);
		//System.out.println(outs.)

	}

	@Test
	public void TestAvroDeserialization() throws IOException {
		DatumReader<User> reader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> filereader = new DataFileReader<User>(new File(
				"a.avo"), reader);

		while (filereader.hasNext()) {
			User u = filereader.next();
			System.out.println(u);
		}

	}

}
