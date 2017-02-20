package com.bigdata.acadgild;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CopyFileLocalToHdfs {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Kindly enter 2 arguments");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FSDataOutputStream fsDataOutputStream = null;
		try {
			InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(args[0])));
			URI hdfsPath = URI.create(args[1]);
			FileSystem fileSystem = FileSystem.get(hdfsPath, conf);
			Path path = new Path(args[1]);
			fsDataOutputStream = fileSystem.create(path);
			IOUtils.copyBytes(inputStream, fsDataOutputStream, conf);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fsDataOutputStream);
		}
	}

}
