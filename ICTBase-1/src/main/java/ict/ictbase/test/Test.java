package ict.ictbase.test;

public class Test {
	 private static boolean initialized;
	public static void main(String args[]) {
		char temp = 99;
		String str = String.valueOf((char) (temp + 1));
		System.out.println(temp);
		System.out.println(str);
		if(initialized==false){
			System.out.println(6666666);
		}else{
			System.out.println(33333);
		}
		 
	}
}
