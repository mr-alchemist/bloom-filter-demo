import java.io.*;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;


public class Program {

	public Program() {
		
	}

	public static void main(String[] args) {
		Program pr = new Program();
		pr.run();
	}
	
	void run() {
		generateDataSet("1.avi");
		testFilter();
		
	}
	int msgLength = 100;
	int msgCount = 10_000_000;
	
	/**
	 * Функция генерирует 3 файла: 
	 * data.bin - датасет с точно неповторяющимися блоками данных(всего их msgCount по msgLength байт каждый)
	 * exist.bin - набор неповторяющихся блоков (по msgLength байт каждый), которые точно есть в data.bin
	 * notexist.bin - набор неповторяющихся блоков (по msgLength байт каждый), которых точно нет в data.bin
	 * @param inputFileName - имя файла, используемого для генерации датасета
	 */
	void generateDataSet(String inputFileName) {
		
		int msgPrefixLength = msgLength - 4;
		
		String outputFileName = "data.bin";
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream(inputFileName));
			BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(outputFileName));
			byte[] buffer = new byte[msgLength];
			
			for(int i = 0; i < msgCount; i++) {
				
				//read from input to buffer msgPrefixLength bytes
				if(input.read(buffer, 0, msgPrefixLength) <= 0) {
					input.close();
					input = new BufferedInputStream(new FileInputStream(inputFileName));
					input.read(buffer, 0, msgPrefixLength);
				}
				
				buffer[msgLength - 1] = (byte)((i >> 0)  & 0xff);
				buffer[msgLength - 2] = (byte)((i >> 8)  & 0xff);
				buffer[msgLength - 3] = (byte)((i >> 16) & 0xff);
				buffer[msgLength - 4] = (byte)((i >> 24) & 0xff);
				output.write(buffer);
				
			}
			
			input.close();
			output.close();
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		
		//exist
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream("data.bin"));
			BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream("exist.bin"));
			byte[] buffer = new byte[msgLength];
			int rndMaxStep = msgCount/10000;
			
			while(true) {
				int step = (int)(Math.random()*rndMaxStep) + 1;
				int readed = msgLength;
				for(int i = 0; i < step && readed == msgLength; i++) {
					readed = input.read(buffer);
				}
				if(readed != msgLength)
					break;
				output.write(buffer);
			}
			
			input.close();
			output.close();
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		
		//not exist
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream(inputFileName));
			BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream("notexist.bin"));
			byte[] buffer = new byte[msgLength];
			int rndMaxStep = msgCount/10000;
			
			for(int j = 0; j < 20000;j++) {
				int i = msgCount + j;
				int step = (int)(Math.random()*rndMaxStep) + 1;
				int readed = msgPrefixLength;
				for(int k = 0; k < step && readed == msgPrefixLength; k++) {
					readed = input.read(buffer, 0, msgPrefixLength);
				}
				if(readed != msgPrefixLength) {
					input.close();
					input = new BufferedInputStream(new FileInputStream(inputFileName));
					input.read(buffer, 0, msgPrefixLength);
				}
				
				buffer[msgLength - 1] = (byte)((i >> 0)  & 0xff);
				buffer[msgLength - 2] = (byte)((i >> 8)  & 0xff);
				buffer[msgLength - 3] = (byte)((i >> 16) & 0xff);
				buffer[msgLength - 4] = (byte)((i >> 24) & 0xff);
				output.write(buffer);
			}
			
			
			input.close();
			output.close();
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		System.out.print("Done");
		
	}
	
	void testFilter() {
		
		BloomFilter<byte[]> filter = BloomFilter.create(
				  Funnels.byteArrayFunnel(),
				  10_000_000,
				  0.01);
		
		try {//read data blocks to filter
			BufferedInputStream input = new BufferedInputStream(new FileInputStream("data.bin"));
			byte[] buf = new byte[msgLength];
			int readed = input.read(buf);
			while(readed == msgLength) {
				filter.put(buf);
				readed = input.read(buf);
			}
			System.out.println("file readed");
			input.close();
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		System.out.println("--------------");
		
		try {//check1
			int blockCnt = 0;
			int posRes = 0;
			int falseNegRes = 0;
			BufferedInputStream input = new BufferedInputStream(new FileInputStream("exist.bin"));
			byte[] buf = new byte[msgLength];
			int readed = input.read(buf);
			while(readed == msgLength) {
				blockCnt++;
				if(filter.mightContain(buf))
					posRes++;
				else
					falseNegRes++;
				
				readed = input.read(buf);
			}
			input.close();
			System.out.println("blockCnt: " + blockCnt);
			System.out.println("posRes: " + posRes);
			System.out.println("falseNegRes: " + falseNegRes);
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		
		System.out.println("--------------");
		
		try {//check2
			int blockCnt = 0;
			int falsePosRes = 0;
			int negRes = 0;
			BufferedInputStream input = new BufferedInputStream(new FileInputStream("notexist.bin"));
			byte[] buf = new byte[msgLength];
			int readed = input.read(buf);
			while(readed == msgLength) {
				blockCnt++;
				if(filter.mightContain(buf))
					falsePosRes++;
				else
					negRes++;
				
				readed = input.read(buf);
			}
			input.close();
			System.out.println("blockCnt: " + blockCnt);
			System.out.println("falsePosRes: " + falsePosRes);
			System.out.println("negRes: " + negRes);
		}
		catch(Exception ex) {
			System.out.println(ex.getClass() + ": "+ ex.getMessage());
			return;
		}
		
	}

}
