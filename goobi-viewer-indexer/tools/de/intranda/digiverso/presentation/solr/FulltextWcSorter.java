package de.intranda.digiverso.presentation.solr;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class FulltextWcSorter {

	static File home = new File("D:/digiverso/_data/Weimar/folders");
	static File fulltext = new File("D:/digiverso/_data/Weimar/fulltext");
	static File wc = new File("D:/digiverso/_data/Weimar/wc");

	public static void main(String[] args) {
		File[] dirs = home.listFiles();
		System.out.println(dirs.length + " folders found.");
		for (File dir : dirs) {
			if (dir.isDirectory()) {
				for (File f : dir.listFiles()) {
					File ftDir = new File(fulltext, dir.getName());
					if (!ftDir.exists()) {
						ftDir.mkdirs();
					}
					File wcDir = new File(wc, dir.getName());
					if (!wcDir.exists()) {
						wcDir.mkdirs();
					}
					if (f.getName().endsWith(".txt")) {
						try {
							FileUtils.moveFile(f, new File(ftDir, f.getName()));
						} catch (IOException e) {
							System.out.println("Cannot move '" + f.getAbsolutePath() + "' to '" + ftDir.getAbsolutePath());
							e.printStackTrace();
							System.exit(0);
						}
					} else if (f.getName().endsWith(".xml")) {
						try {
							FileUtils.moveFile(f, new File(wcDir, f.getName()));
						} catch (IOException e) {
							System.out.println("Cannot move '" + f.getAbsolutePath() + "' to '" + wcDir.getAbsolutePath());
							e.printStackTrace();
							System.exit(0);
						}
					}
				}
				if (dir.listFiles().length == 0) {
					try {
						FileUtils.deleteDirectory(dir);
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(0);
					}
				}
				System.out.println(dir.getName() + " done.");
			}
		}
	}
}
