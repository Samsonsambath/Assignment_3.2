package com.bigdata.acadgild;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DisplayFileContent {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Kindly enter atleast 1 argument");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Path path = new Path(args[0]);
		FSDataInputStream fsDataInputStream = null;
		try {
			FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
			fsDataInputStream = fileSystem.open(path);
			IOUtils.copyBytes(fsDataInputStream, System.out, conf);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fsDataInputStream);
		}

	}

}
