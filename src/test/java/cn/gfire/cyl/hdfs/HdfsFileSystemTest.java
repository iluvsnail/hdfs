package cn.gfire.cyl.hdfs;

import java.io.IOException;

import org.junit.Test;

public class HdfsFileSystemTest {

	@Test
	public void testUploadFile() throws IOException {
		HdfsFileSystem.uploadFile("C:\\Users\\Acer\\Desktop\\chart.pdf", "hdfs://master:9000/chenyl/test/");
	}
	
}
