package cn.gfire.cyl.hdfs;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

public class HdfsFileSystemTest {

	@Test
	public void testUploadFile() throws IOException {
		//HdfsFileSystem.uploadFile("C:\\Users\\Acer\\Desktop\\chart.pdf", "hdfs://master:9000/chenyl/test/");
	}
	
	@Test
	public void testDownloadFile() throws IOException {
		HdfsFileSystem.downLoadFile("hdfs://master:9000/chenyl/test/chart.pdf", "C:\\");
		File file = new File("C:\\chart.pdf");
		Assert.assertTrue(file.exists());
	}
	
	@Test
	public void testLs() throws IOException{
		String filePath = "hdfs://master:9000/chenyl/test/chart.pdf";
		HdfsFileSystem.ls(filePath);
	}
}
