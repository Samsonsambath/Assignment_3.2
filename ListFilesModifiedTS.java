package com.bigdata.acadgild;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFilesModifiedTS {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Kindly enter atleast 1 parameter");
		}
		Configuration conf = new Configuration();
		try {
			Path path = new Path(args[0]);
			listFiles(path, conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void listFiles(Path path, Configuration conf) throws FileNotFoundException, IOException {
		FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
		FileStatus[] filestatus = fileSystem.listStatus(path);

		for (FileStatus fs : filestatus) {
			if (fs.isFile()) {
				System.out.println("File Name is ***" + fs.getPath().getName() + " LastModified timestamp is ***"
						+ new Timestamp(fs.getModificationTime()));
			} else if (fs.isDirectory()) {
				System.out.println(fs.getPath().getName() + " is a Directory");
				System.out.println(" following are the files");
				listFiles(fs.getPath(), conf);
			}
		}
	}
}
