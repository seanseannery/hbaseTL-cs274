package gov.pentagon.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProjectPreferences extends Properties {
	private static final long serialVersionUID = -3772903052918327405L;

	public static final String HBASE_CONFIG_PATH = "hbase_config_path";

	private static final String PREFS_FILE_PATH = "res/preferences.txt";

	private ProjectPreferences() throws IOException {
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(new File(PREFS_FILE_PATH));
			load(fileInputStream);
		} finally {
			if (fileInputStream != null)
				fileInputStream.close();
		}
	}

	public static ProjectPreferences getPrefs() {
		return SingletonContainer.singleton;
	}

	private static final class SingletonContainer {
		private static final ProjectPreferences singleton;
		
		static {
			try {
				singleton = new ProjectPreferences();
			} catch (IOException e) {
				System.err.println("Error operating on the preferences file! Assumed to be in \"" + new File(PREFS_FILE_PATH).getAbsolutePath() + "\"");
				throw new RuntimeException(e);
			}
		}
	}
}
