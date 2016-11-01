/**
 * 
 */
package ict.ocrabase.main.java.client.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ict.ocrabase.main.java.client.bulkload.BulkLoad;
import ict.ocrabase.main.java.client.bulkload.TableInfo;
//-s /lr/data1 -ts testtable,,5,f1:c1:String,f2:c2:String,f2:c3:String -l 5
/**
 * @author Liu Wei
 * 
 */
public class Import_1 extends Cli {

	public static void main(String[] args) {
		try {
			Import_1 imp = new Import_1();
			opt = new Options();

			imp.setFormat();
			cli = new GnuParser().parse(opt, args);

			imp.checkArgs();

			String type = null;
			if (cli.hasOption("l")) {
				type = "i " + cli.getOptionValue("l");
			} else {
				type = "u";
			}

			BulkLoad bl = null;
			if (isFilePath()) {
				bl = new BulkLoad(imp.getTableStruct(),
						cli.getOptionValue("s"), type);
			} else {
				TableInfo ts = new TableInfo(imp.dealTableStruct(cli
						.getOptionValue("ts")));
				bl = new BulkLoad(ts, cli.getOptionValue("s"), type);
			}
			if(cli.hasOption("p")) {
				bl.preTest();
			}
			bl.run();
			while (!bl.isComplete()) {
				System.out.print("\r");
				System.out.print("process: " + bl.getProgress() + "%");
				Thread.sleep(5000);
			}
			System.out.println("process: " + bl.getProgress() + "%");
			System.out.println(bl.getResult());
		} catch (ParseException e) {
			System.err.println("Invaild arguments!");
			printHelp();
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected void setFormat() {
		OptionBuilder.hasArg();
		OptionBuilder.withLongOpt("source");
		OptionBuilder.withArgName("hdfs path");
		OptionBuilder
				.withDescription("the source data dictionary path in HDFS");
		Option source = OptionBuilder.create("s");
		opt.addOption(source);

		OptionBuilder.hasArg();
		OptionBuilder.withLongOpt("tablestruct");
		OptionBuilder.withArgName("structure or file path");
		OptionBuilder.withDescription("the structure of ictbase table");
		Option tablestruct = OptionBuilder.create("ts");
		opt.addOption(tablestruct);

		OptionBuilder.hasArg();
		OptionBuilder.withLongOpt("rowkeylen");
		OptionBuilder.withArgName("length");
		OptionBuilder.withDescription("the length of rowkey");
		Option rowkeylen = OptionBuilder.create("l");
		opt.addOption(rowkeylen);
		
		OptionBuilder.withLongOpt("preTest");
		OptionBuilder.withArgName("length");
		OptionBuilder.withDescription("to start a pre test before the program start.");
		Option preTest = OptionBuilder.create("p");
		opt.addOption(preTest);

		OptionBuilder.withLongOpt("order");
		OptionBuilder.withDescription("rowkey is given by user data in order");
		Option order = OptionBuilder.create("o");
		opt.addOption(order);

		OptionBuilder.withLongOpt("disorder");
		OptionBuilder
				.withDescription("rowkey is given by user data in disorder");
		Option disorder = OptionBuilder.create("do");
		opt.addOption(disorder);

		OptionBuilder.withLongOpt("help");
		OptionBuilder.withDescription("print this message");
		Option help = OptionBuilder.create("h");
		opt.addOption(help);
	}

	protected void checkArgs() {
		if (cli.hasOption("h")) {
			if (cli.hasOption("-s") || cli.hasOption("-ts")
					|| cli.hasOption("-o") || cli.hasOption("-l")
					|| cli.hasOption("-do")) {
				System.out.println(" '-h' must be used alone!\n");
				System.exit(-1);
			} else {
				Import_1.printHelp();
				System.exit(-1);
			}
		}
		if (!(cli.hasOption("s") || cli.hasOption("ts") || cli.hasOption("h")
				|| cli.hasOption("l") || cli.hasOption("o") || cli
					.hasOption("do"))) {
			System.err.println("Invaild arguments or no order!");
			Import_1.printHelp();
			System.exit(-1);
		}
		if (cli.hasOption("s") && cli.hasOption("ts")) {
			if ((cli.hasOption("l") && cli.hasOption("o"))
					|| (cli.hasOption("do") && cli.hasOption("o"))
					|| (cli.hasOption("l") && cli.hasOption("do"))) {
				System.out
						.println(" Only one of '-l','-o','-do' can exist in one sentence!");
				System.exit(-1);
			}
		} else if (!cli.hasOption("s")) {
			if (!cli.hasOption("ts")) {
				System.out
						.println("Both of '-s' and '-ts' must exist if one of '-l','-o','-do' exists!");
				System.exit(-1);
			}
			System.out.println("'-s' must exist if '-ts' exists");
			System.exit(-1);
		} else if (cli.hasOption("s")) {
			System.out.println("'-ts' must exist if '-s' exists");
			System.exit(-1);
		}
	}

	public static void printHelp() {
		System.out
				.println("\nDescription: this order is used to import data\n");
		// HelpFormatter formatter = new HelpFormatter();
		// formatter.printHelp("import.sh", opt, true);
		//
		System.out
				.println("usage: import.sh [-s<hdfs path> -ts<structure or file path> [-l<length>]] |\n"
						+ "                 [-h<help>]\n");
		System.out
				.println("-h,--help                                    print this message\n");
		System.out
				.println("-l,--rowkeylen <length>                      the length of rowkey\n");
		System.out
				.println("-s,--source <hdfs path>                      the source data dictionary path\n"
						+ "                                             in HDFS\n");
		System.out
				.println("-ts,--tablestruct <structure or file path>   the structure of ictbase table\n");
		System.out.println("Addition:\n");
		System.out
				.println(" 1. If '-s' exists,then '-ts' must exist too!\n");
		System.out.println(" 2. '-h' must be used alone!\n");
		System.out
				.println(" 3. tablestruct[-ts] should be in type of 'tablename,COMMA | TAB | SEMICOLON |\n"
						+ "    SPACE,field1[,field2,...]'\n");
		System.out.println("\t3.1, Space is forbidden in the tablestruct\n");
		System.out
				.println("\t3.2, Separator in the import data should be one of 'COMMA' , 'TAB' ,\n"
						+ "\t     'SEMICOLON' or 'SPACE' , standing for ',' , '\\t' , ';' , ' '\n");
		System.out
				.println("\t3.3, field1[,field2,...] are strings of field description,at least one\n"
						+ "\t     filed must be there\n");
		System.out.println("\t3.4, ',' are used to separate two fields\n");
		System.out.println("\t3.5, tablestruct supports file path importing\n");
		System.out
				.println("\t3.6, the fields should be in type of 'Family name:Qualifier name:Type\n");
		System.out
				.println("\t   3.6.1  Type should be one of 'STRING' , 'INT' , 'DOUBLE' , 'LONG'\n");
		System.out
				.println("\nExample(without index):./import.sh -s hdfs://lingcloud34/test -ts t,COMMA,f:uuid:STRING,f:gender:INT,f:name:STRING,f:score:INT,f:payment:DOUBLE -l 32");
		System.out
				.println("\nExample(with index):./import.sh -s /test -ts t,COMMA,f:gender:INT:CCINDEX:_t,f:name:STRING,f:score:INT,f:payment:DOUBLE -l 32\n");
	}

	private static boolean isFilePath() {
		String str = cli.getOptionValue("ts");
		File file = new File(str);
		if (file.exists() && file.isFile()) {
			return true;
		}
		return false;
	}

	private String getTableStruct() throws IOException {
		File file = new File(cli.getOptionValue("ts"));
		BufferedReader reader;
		String ts = null;
		reader = new BufferedReader(new FileReader(file));
		ts = reader.readLine();
		return dealTableStruct(ts);
	}

	private String dealTableStruct(String ts) {
		String newTs = "";
		String[] splitTs;
		splitTs = ts.split(",");
		int ascii = 0;
		if (splitTs[1].equalsIgnoreCase("COMMA")) {
			ascii = 44;
		} else if (splitTs[1].equalsIgnoreCase("TAB")) {
			ascii = 9;
		} else if (splitTs[1].equalsIgnoreCase("SEMICOLON")) {
			ascii = 59;
		} else if (splitTs[1].equalsIgnoreCase("SPACE")) {
			ascii = 32;
		} else {
			try{
				ascii = Integer.parseInt(splitTs[1]);
				if(ascii < 0 || ascii >= 128)
					ascii = 32;
			} catch(NumberFormatException e){
				ascii = 32;
			}
			
		}
		newTs = splitTs[0] + "," + ascii;
		for (int i = 2; i < splitTs.length; i++) {
			newTs = newTs + "," + splitTs[i];
		}
		return newTs;
	}

	private static Options opt;
	private static CommandLine cli;
}
