package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
	ContentValues values = new ContentValues();
	private static final String KEY_FIELD_DB = "key";
	private static final String VALUE_FIELD_DB = "value";
	private static final String o_FIELD_DB = "owner";
	static  String myPort;

	ArrayList<String> lk=new ArrayList<String>();
	ArrayList<String> lv=new ArrayList<String>();
	TreeMap avdMap=new TreeMap<String,String>();
	static int SERVER_PORT=10000;
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	LinkedList<String> avd=new LinkedList<String>();
	LinkedList<String> hashavd= new LinkedList<String>();
	private final Lock lock=new ReentrantLock();
	public class DhtHelper extends SQLiteOpenHelper
	{

		private static final String DATABASE_NAME = "groupTable";
		private static final int DATABASE_VERSION = 2;
		public static final String key = "key";
		public static final String value = "value";
		public static final String owner = "owner";
		public static final String dummy = "dummy";

		private static final String TAG = "GroupMessengerDB";
		public static final String TABLE = "Dht1";
		public static final String TABLE1 = "Dht2";
		public static final String TABLE2 = "Dht3";
		public static final String TABLE3 = "Dht4";
		public static final String TABLE4 = "Dht5";
		public static final String TABLE5 = "Dht6";
		private static final String table1="CREATE TABLE "+TABLE+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		private static final String table2="CREATE TABLE "+TABLE1+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		private static final String table3="CREATE TABLE "+TABLE2+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		private static final String table4="CREATE TABLE "+TABLE3+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		private static final String table5="CREATE TABLE "+TABLE4+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		private static final String table6="CREATE TABLE "+TABLE5+"(key VARCHAR PRIMARY KEY,owner VARCHAR,value VARCHAR);";
		DhtHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(table1);
			db.execSQL(table2);
			db.execSQL(table3);
			db.execSQL(table4);
			db.execSQL(table5);
			db.execSQL(table6);

			/*db.execSQL("CREATE TABLE if not exists " + TABLE + " (" +
					key + " ," +
					owner+ " ," +value+  ");");
			db.execSQL("CREATE TABLE if not exists " + TABLE1 + " (" +
					key + " ," +
					owner+ " ," +value+ ");");
			db.execSQL("CREATE TABLE if not exists " + TABLE2 + " (" +
					key + " ," +
					owner+ " ," +value+ ");");
			db.execSQL("CREATE TABLE if not exists " + TABLE3+ " (" +
					key + " ," +
					owner+ " ," +value+  ");");
			db.execSQL("CREATE TABLE if not exists " + TABLE4 + " (" +
					key + " ," +
					owner+ " ," +value+  ");");
			db.execSQL("CREATE TABLE if not exists " + TABLE5 + " (" +
					key + " ," +
					owner+ " ," +value+  ");");
			*/
		}


		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			Log.w(TAG, "Upgrading db from  " + oldVersion + " to "
					+ newVersion + ", delete old table");
			db.execSQL("DROP TABLE IF EXISTS " + TABLE);
			onCreate(db);
		}

	}


	DhtHelper dbHelper;
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity");
	@Override
	public boolean onCreate() {
		lock.lock();
		dbHelper=new DhtHelper(getContext());
		// TODO Auto-generated method stub
		try {


			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.i(TAG,"Server task created");
		}
		catch (IOException e) {
			Log.e(TAG, "server not able create ServerSocket");

			e.printStackTrace();

		}

		for (String remote_port : REMOTE_PORT) {
			try{
				avd.add(String.valueOf(Integer.parseInt(remote_port)/2));
				hashavd	.add(genHash(String.valueOf(Integer.parseInt(remote_port)/2)));
				avdMap.put(genHash(String.valueOf(Integer.parseInt(remote_port)/2)),String.valueOf(Integer.parseInt(remote_port)/2));

			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		Collections.sort(hashavd);

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));


		Cursor cursor =null;
		SQLiteDatabase db = dbHelper.getReadableDatabase();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(DhtHelper.TABLE);
		cursor = queryBuilder.query(db, null, null,
				null, null, null, null);

		if(cursor.getCount()>=0) {
			String msg="";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,myPort);
			Log.i(TAG, "Client created");
		}
		else{
			lock.unlock();
		}
		return false;
	}




	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		SQLiteDatabase db = dbHelper.getReadableDatabase();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(DhtHelper.TABLE);
		String[] s5 = {selection};
		String s4 = DhtHelper.key + " = ?";
		String msgTosend2 = selection + ":" + "Delete";
		Socket s2 = null;
		for ( String p1: REMOTE_PORT) {
			try {
				s2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p1));
				PrintWriter msgToServer = new PrintWriter(s2.getOutputStream(), true);
				msgToServer.write(msgTosend2);
				msgToServer.println();
				msgToServer.flush();
				//Log.i("Insert Fwd", "Msg sent to88 " + rep2portnumber);

				BufferedReader readline = new BufferedReader(new InputStreamReader(s2.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				Log.i("Insert Fwd", "Msg sent tosampreeth88" + ack);
				s2.close();

				//String [] ackp=ack.split("$");

				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s2.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s2.close();
				}

			} catch (Exception e) {
				e.printStackTrace();

			}
			if (selection.contains("@")) {
				db.delete(DhtHelper.TABLE, null, null);
				db.delete(DhtHelper.TABLE1, null, null);


			} else if (selection.contains("*")) {
				db.delete(DhtHelper.TABLE, null, null);
			} else {
				String s1 = DhtHelper.key + " like '" + selection + "'";
				db.delete(DhtHelper.TABLE, null, null);
			}
			// TODO Auto-generated method stub
		}
		return 0;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = (String) values.get(KEY_FIELD_DB);
		String Value = (String) values.get(VALUE_FIELD_DB);
		String hashKey = "";
		String avdhashtoinsert = "";
		String avdporttoinsert = "";
		try {
			hashKey = genHash(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < hashavd.size(); i++) {
			Log.i("Insert Fwd", "Msg sent to568987 " + avdMap.get(hashavd.get(i)));

		}
		for (int i = 0; i < hashavd.size(); i++) {
			int comp1 = hashKey.compareTo(hashavd.get(i));
			if (i == 0) {

				int comp2 = hashKey.compareTo(hashavd.getLast());
				if (comp1 <= 0 && comp2 > 0) {
					avdhashtoinsert = hashavd.get(i);
					break;
				}
			} else {

				int comp2 = hashKey.compareTo(hashavd.get(i - 1));
				if (comp1 <= 0 && comp2 > 0) {
					avdhashtoinsert = hashavd.get(i);
					break;
				}

			}

		}
		if(avdhashtoinsert==""){
			avdhashtoinsert = hashavd.get(0);
		}
		Log.i("Insert Fwd", "Msg sent to567 " + key +"    " +Value);
		Log.i("Insert Fwd", "Msg sent to566 " + avdhashtoinsert);
		avdporttoinsert = avdMap.get(avdhashtoinsert).toString();
		String portnumber = "";
		portnumber = String.valueOf(Integer.parseInt(avdporttoinsert) * 2);
		int avdindex = hashavd.indexOf(avdhashtoinsert);
		int nextindex = 0;
		int nextnextindex = 0;
		for (int j = 0; j < hashavd.size(); j++) {
			Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);
			if (avdindex == j && avdindex == hashavd.size() - 1) {
				nextindex = 0;
				nextnextindex = 1;
				Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);


			} else if (avdindex == j && avdindex == hashavd.size() - 2) {
				nextindex = hashavd.size() - 1;
				nextnextindex = 0;
				Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);

			}
			else if (j==avdindex){
				nextindex = j+1;
				nextnextindex = j+2;
				Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);

			}
		}
		Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);
		String rep1port = avdMap.get(hashavd.get(nextindex)).toString();
		String rep2port = avdMap.get(hashavd.get(nextnextindex)).toString();
		Log.i("Insert Fwd", "Msg sent to2" + rep1port  +   rep2port);
		String rep1portnumber = String.valueOf(Integer.parseInt(rep1port) * 2);
		String rep2portnumber = String.valueOf(Integer.parseInt(rep2port) * 2);
		Log.i("Insert Fwd", "Msg sent to3" + rep1portnumber +rep2portnumber + portnumber);
		String[] list = {portnumber, rep1portnumber, rep2portnumber};
		String msgTosend = key + ":" + Value + ":" + "Insert"+":"+portnumber;
		for (String port : list) {
			Socket s = null;
			try {
				s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				PrintWriter msgToServer = new PrintWriter(s.getOutputStream(), true);
				msgToServer.write(msgTosend);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Insert Fwd", "Msg sent to666 " + port);

				BufferedReader readline = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				Log.i("Insert Fwd", "Msg sent to661 " + ack);
				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s.close();
				}

			} catch (Exception e) {
				e.printStackTrace();

			}

		}


		// TODO Auto-generated method stub
		return null;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			String str = "";
			Socket s;
			Cursor cursor=null;
			try{
				while (true){
					lock.lock();
					lock.unlock();
					s=serverSocket.accept();
					BufferedReader read = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String ack="";
					ack=read.readLine();
					if(ack==null||ack==""){
						continue;
					}

					String[] parts = ack.split(":");

					Log.i("Insert received", "doing insert999"+ack);

					if(ack.contains("Insert") && parts.length==4){
						SQLiteDatabase db = dbHelper.getWritableDatabase();
						String k=parts[0];
						String v=parts[1];
						String o=parts[3];
						ContentValues cv=new ContentValues();
						cv.put(KEY_FIELD_DB,k);
						cv.put(VALUE_FIELD_DB,v);
						cv.put(o_FIELD_DB,o);
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						Log.i("Insert received", "doing done1234"+k+" "+v+" "+o);
						db.insertWithOnConflict(DhtHelper.TABLE,null,cv,db.CONFLICT_REPLACE);
						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write("PA4 OK");
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Insert received", "doing done1");
						s.close();
					}
					else if(ack.contains("Delete") ){
						Log.i("Delete received", "doing done33");
						SQLiteDatabase db = dbHelper.getWritableDatabase();

						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						db.delete(DhtHelper.TABLE,null,null);
						db.delete(DhtHelper.TABLE1,null,null);
						db.delete(DhtHelper.TABLE2,null,null);
						db.delete(DhtHelper.TABLE3,null,null);
						db.delete(DhtHelper.TABLE4,null,null);
						db.delete(DhtHelper.TABLE5,null,null);

						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write("PA4 OK");
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Insert received", "doing done1");
						s.close();
					}
					else if(ack.contains("Query") && parts.length==2){
						String selection=parts[0];
						String [] selectionArgs={selection};
						SQLiteDatabase db = dbHelper.getReadableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						String s1 = DhtHelper.key + "=?";
						Log.v("Query selection string", s1);
						cursor = queryBuilder.query(db, null, s1, selectionArgs, null, null, null);
						String kvinthisnode="";
						if(cursor.moveToFirst()){

							kvinthisnode=cursor.getString(0)+"#"+cursor.getString(2);

						}
						Log.v("Query 12345", kvinthisnode);
						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write(kvinthisnode);
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Query received", "doing done2");
						s.close();
					}
					else if(ack.contains("allsum") ){
						Log.i("Query all received", "all doing done3");
						String selection=parts[0];
						String [] selectionArgs={selection};
						SQLiteDatabase db = dbHelper.getReadableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						String s1 = DhtHelper.key + "=?";
						Log.v("Query selection string", s1);
						cursor = queryBuilder.query(db, null, null, null, null, null, null);
						String kvinthisnode="";
						String temp;
						if(cursor.moveToFirst()){
							while(!cursor.isAfterLast()){
								Log.i("Query 2222", cursor.getString(1));
								if(cursor.isLast()) {
									temp=cursor.getString(0)+" "+cursor.getString(2);
								}
								else{
									temp=cursor.getString(0)+" "+cursor.getString(2)+" ";
								}
								kvinthisnode+=temp;
								cursor.moveToNext();
							}
						}
						Log.v("Query selection string", kvinthisnode);
						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write(kvinthisnode);
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Query received", "doing done4");
						s.close();
					}
					else if(ack.contains("SendMyData") && parts.length==3){
						Log.i("Query received", "doing 4444");
						String p1=parts[1];
						String p2=parts[2];
						SQLiteDatabase db = dbHelper.getReadableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						String s1 = DhtHelper.key + "=?";
						Log.v("Query selection string", s1);
						cursor = queryBuilder.query(db, null, null, null, null, null, null);
						String kvinthisnode="";
						String temp;
						Log.i("Query received", "doing 4445"+" "+p1+" "+p2+" "+cursor.getCount());
						if(cursor.moveToFirst()){

							while(!cursor.isAfterLast() ) {
								Log.i("Query received", "doing 4447"+"   "+cursor.getString(1)+" "+cursor.getString(0));
								if (cursor.getString(1).equalsIgnoreCase(p1)||cursor.getString(1).equalsIgnoreCase(p2)) {
									if (cursor.isLast()) {
										temp = cursor.getString(0) + " " + cursor.getString(2)+" "+cursor.getString(1);
									} else {
										temp = cursor.getString(0) + " " + cursor.getString(2) + " "+cursor.getString(1)+" ";
									}
									kvinthisnode += temp;

								}
								cursor.moveToNext();
							}
						}
						Log.i("Query received", "doing 4446");
						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write(kvinthisnode);
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Query received", "doing gob1");
						s.close();
					}
					else if(ack.contains("CollectMyData") && parts.length==3){
						String p1=parts[1];
						SQLiteDatabase db = dbHelper.getReadableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(DhtHelper.TABLE);
						String s1 = DhtHelper.key + "=?";
						Log.v("Query selection string", s1);
						cursor = queryBuilder.query(db, null, null, null, null, null, null);
						String kvinthisnode="";
						String temp;
						if(cursor.moveToFirst()){
							while(!cursor.isAfterLast() ) {
								if (cursor.getString(1).equalsIgnoreCase(p1)) {
									if (cursor.isLast()) {
										temp = cursor.getString(0) + " " + cursor.getString(2)+" "+cursor.getString(1);
									} else {
										temp = cursor.getString(0) + " " + cursor.getString(2) + " "+cursor.getString(1)+" ";
									}
									kvinthisnode += temp;

								}
								cursor.moveToNext();
							}
						}
						PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
						msgToServer1.write(kvinthisnode);
						msgToServer1.println();
						msgToServer1.flush();
						Log.i("Query received", "doing gob2");
						s.close();
					}
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}

			return null;

		}


	}
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String thishash="";
			try{
				thishash=genHash(String.valueOf(Integer.parseInt(myPort)/2));
			}
			catch (Exception e){
				e.printStackTrace();
			}
			int myindex=hashavd.indexOf(thishash);
			int nextindex=0;
			int previndex=0;
			int prevprevindex=0;
			if(myindex==4){
				nextindex=0;
				previndex=3;
				prevprevindex=2;
			}
			else if(myindex==0){
				nextindex=1;
				previndex=4;
				prevprevindex=3;
			}
			else if(myindex==1){
				nextindex=2;
				previndex=0;
				prevprevindex=4;
			}
			else{
				nextindex=myindex+1;
				previndex=myindex-1;
				prevprevindex=myindex-2;
			}
			String nextavdnumber=String.valueOf(avdMap.get(hashavd.get(nextindex)));
			String prevavdnumber=String.valueOf(avdMap.get(hashavd.get(previndex)));
			String prevprevavdnumber=String.valueOf(avdMap.get(hashavd.get(prevprevindex)));
			Log.i("Recovery received", "doing recovery1"+nextavdnumber+prevavdnumber+prevprevavdnumber);

			String nextportnumber=String.valueOf(Integer.parseInt(nextavdnumber)*2);
			String prevportnumber=String.valueOf(Integer.parseInt(prevavdnumber)*2);
			String prevprevportnumber=String.valueOf(Integer.parseInt(prevprevavdnumber)*2);
			Log.i("Recovery received", "doing recovery2"+nextportnumber+prevportnumber+prevprevavdnumber);

			String m="SendMyData"+":"+prevportnumber+":"+myPort;
			Socket s = null;
			SQLiteDatabase db = dbHelper.getWritableDatabase();
			SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
			queryBuilder.setTables(DhtHelper.TABLE);
			try {
				s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextportnumber));
				PrintWriter msgToServer = new PrintWriter(s.getOutputStream(), true);
				msgToServer.write(m);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Recovery received", "doing recovery3"+nextportnumber);


				BufferedReader readline = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String ack = "";
				ack = readline.readLine();

				if(ack!=null && ack!=""& ack!=" ") {
					String[] parts=ack.trim().split(" ");
					Log.i("Recovery received", "doing recovery4");
					for (int i1 = 0; i1 < parts.length; i1 = i1 + 3) {

						ContentValues values = new ContentValues();
						if(i1+2<=parts.length) {
							Log.i("return888",  "f"+ parts[i1]+" "+parts[i1+1]+" "+parts[i1+2]);
							values.put(KEY_FIELD_DB, parts[i1]);
							values.put(VALUE_FIELD_DB, parts[i1 + 1]);
							values.put(o_FIELD_DB,parts[i1+2]);
							db.insertWithOnConflict(DhtHelper.TABLE, null, values,db.CONFLICT_REPLACE);
						}

					}
				}
				s.close();

				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s.close();
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
			String m1="CollectMyData"+":"+prevprevportnumber+":"+nextportnumber;
			Socket s1 = null;
			try {
				s1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(prevportnumber));
				PrintWriter msgToServer = new PrintWriter(s1.getOutputStream(), true);
				msgToServer.write(m1);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Recovery received", "doing recovery6"+nextportnumber);


				BufferedReader readline = new BufferedReader(new InputStreamReader(s1.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				if(ack!=null &&ack!="" && ack!= " ") {
					String[] parts = ack.trim().split(" ");
					Log.i("Recovery received", "doing recovery7");
					for (int i1 = 0; i1 < parts.length; i1 = i1 + 3) {
						Log.i("return5", ack.length() + "     " + parts.length);
						ContentValues values = new ContentValues();
						if(i1+2<=parts.length) {
							Log.i("return889",  "f"+ parts[i1]+" "+parts[i1+1]+" "+parts[i1+2]);
							values.put(KEY_FIELD_DB, parts[i1]);
							values.put(VALUE_FIELD_DB, parts[i1 + 1]);
							values.put(o_FIELD_DB,parts[i1+2]);
							db.insertWithOnConflict(DhtHelper.TABLE, null, values,db.CONFLICT_REPLACE);
						}

					}
				}
				s1.close();

				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s1.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s1.close();
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}






			return null;
		}
		@Override
		protected void onPostExecute(Void aVoid) {
			super.onPostExecute(aVoid);
			lock.unlock();
		}

	}




	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		Cursor cursor =null;
		Log.i("Insert Fwd", "Msg sent tosampreeth00" + " "+selection);

		SQLiteDatabase db = dbHelper.getReadableDatabase();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(DhtHelper.TABLE);
		lock.lock();
		lock.unlock();
		if(selection.equalsIgnoreCase("@")){
			cursor = queryBuilder.query(db, null, null,
					null, null, null, null);
			if(cursor.moveToFirst()) {
				while (!cursor.isAfterLast()) {
					Log.i("Insert Fwd", "Msg sent tosampreeth11" + cursor.getString(0));
					cursor.moveToNext();
				}
			}
		}
		else if(selection.equalsIgnoreCase("*")){
			SQLiteDatabase db2 = dbHelper.getWritableDatabase();
			queryBuilder.setTables(DhtHelper.TABLE2);
			for(int i=0;i<hashavd.size();i++){
				String p=String.valueOf(Integer.parseInt(avdMap.get(hashavd.get(i)).toString())*2);
				String msg =  selection + ":" + "allsum";
				Socket s = null;
				try {
					s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p));
					PrintWriter msgToServer = new PrintWriter(s.getOutputStream(), true);
					msgToServer.write(msg);
					msgToServer.println();
					msgToServer.flush();
					Log.i("Insert Fwd", "Msg sent to " + p);

					BufferedReader readline = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String ack = "";

					ack = readline.readLine();
					Log.i("Insert Fwd", "Msg sent to333" + ack);
					String [] s3Parts=ack.trim().split(" ");
					Log.i("return3",ack+"     "+s3Parts.length);
					for (int i1=0;i1<s3Parts.length;i1=i1+2){
						Log.i("return5",ack+"     "+s3Parts.length);
						ContentValues values=new ContentValues();
						values.put(KEY_FIELD_DB,s3Parts[i1]);
						values.put(VALUE_FIELD_DB,s3Parts[i1+1]);
						db2.insertWithOnConflict(DhtHelper.TABLE2, null, values,db2.CONFLICT_REPLACE);
						getContext().getContentResolver().notifyChange(uri, null);

					}

					s.close();

					if (ack == null)
					{
						Log.i(TAG, "Connection closed");
						s.close();
					} else if (ack.contains("PA4 OK")) {
						Log.i(TAG, "AVD replied. Insert Ok");
						s.close();
					}

				} catch (Exception e) {
					e.printStackTrace();

				}

			}
			cursor = queryBuilder.query(db2, null, null,
					null, null, null, null);
		}
		else {
			Cursor c1=null;
			Cursor c2=null;
			Cursor c3=null;

			String hashKey = "";
			String avdhashtoinsert = "";
			String avdporttoinsert = "";
			try {
				hashKey = genHash(selection);
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (int i = 0; i < hashavd.size(); i++) {
				Log.i("Insert Fwd", "Msg sent to568987 " + avdMap.get(hashavd.get(i)));

			}
			for (int i = 0; i < hashavd.size(); i++) {
				int comp1 = hashKey.compareTo(hashavd.get(i));
				if (i == 0) {

					int comp2 = hashKey.compareTo(hashavd.getLast());
					if (comp1 <= 0 && comp2 > 0) {
						avdhashtoinsert = hashavd.get(i);
						break;
					}
				} else {

					int comp2 = hashKey.compareTo(hashavd.get(i - 1));
					if (comp1 <= 0 && comp2 > 0) {
						avdhashtoinsert = hashavd.get(i);
						break;
					}

				}

			}
			if (avdhashtoinsert == "") {
				avdhashtoinsert = hashavd.get(0);
			}


			avdporttoinsert = avdMap.get(avdhashtoinsert).toString();
			String portnumber = "";
			portnumber = String.valueOf(Integer.parseInt(avdporttoinsert) * 2);
			int avdindex = hashavd.indexOf(avdhashtoinsert);
			int nextindex = 0;
			int nextnextindex = 0;
			for (int j = 0; j < hashavd.size(); j++) {
				Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);
				if (avdindex == j && avdindex == hashavd.size() - 1) {
					nextindex = 0;
					nextnextindex = 1;
					Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);


				} else if (avdindex == j && avdindex == hashavd.size() - 2) {
					nextindex = hashavd.size() - 1;
					nextnextindex = 0;
					Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);

				}
				else if (j==avdindex){
					nextindex = j+1;
					nextnextindex = j+2;
					Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);

				}
			}
			Log.i("Inside for loop", "index" + avdindex  +   nextindex+ nextnextindex);
			String rep1port = avdMap.get(hashavd.get(nextindex)).toString();
			String rep2port = avdMap.get(hashavd.get(nextnextindex)).toString();
			Log.i("Insert Fwd", "Msg sent to002"+" "+selection+" " + rep1port  +   rep2port);
			String rep1portnumber = String.valueOf(Integer.parseInt(rep1port) * 2);
			String rep2portnumber = String.valueOf(Integer.parseInt(rep2port) * 2);
			Log.i("Insert Fwd", "Msg sent to003" +" "+selection+" "+ rep1portnumber +rep2portnumber);
			String[] list = {portnumber, rep1portnumber, rep2portnumber};
			String msgTosend = selection + ":" + "Query";
			Socket s = null;
			try {
				queryBuilder.setTables(DhtHelper.TABLE3);
				SQLiteDatabase db3 = dbHelper.getWritableDatabase();
				s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portnumber));
				PrintWriter msgToServer = new PrintWriter(s.getOutputStream(), true);
				msgToServer.write(msgTosend);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Insert Fwd", "Msg sent to55 " + portnumber);

				BufferedReader readline = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				Log.i("Insert Fwd", "Msg sent tosampreeth55" + ack);
				s.close();

				if(ack!=null) {//String [] ackp=ack.split("$");
					String[] ack1 = ack.split("#");
					Log.i("Insert Fwd", "Msg sent tosampreeth55" + ack1[1]);
					ContentValues cv = new ContentValues();
					cv.put(KEY_FIELD_DB, ack1[0]);
					cv.put(VALUE_FIELD_DB, ack1[1]);
					String s1 = DhtHelper.key + " like '" + selection + "'";
					db3.insertWithOnConflict(DhtHelper.TABLE3, null, cv,db3.CONFLICT_REPLACE);
					Log.i("Insert Fwd", "Msg sent tosampreeth55" + ack1[0] + " " + ack1[1]);
					c1 = queryBuilder.query(db3, null, s1, selectionArgs, null, null, null);
					if(c1.moveToFirst()) {
						Log.i("Insert Fwd", "Msg sent tosampreeth55" + c1.getString(0));
					}
				}

				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s.close();
				}

			} catch (Exception e) {
				e.printStackTrace();

			}
			String msgTosend1 = selection + ":" + "Query";
			Socket s1 = null;
			try {
				queryBuilder.setTables(DhtHelper.TABLE4);
				SQLiteDatabase db4 = dbHelper.getWritableDatabase();
				s1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rep1portnumber));
				PrintWriter msgToServer = new PrintWriter(s1.getOutputStream(), true);
				msgToServer.write(msgTosend1);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Insert Fwd", "Msg sent to77 " + rep1portnumber);

				BufferedReader readline = new BufferedReader(new InputStreamReader(s1.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				Log.i("Insert Fwd", "Msg sent tosampreeth77" + ack);
				s1.close();

				//String [] ackp=ack.split("$");
				if(ack!=null) {
					String[] ack1 = ack.split("#");
					Log.i("Insert Fwd", "Msg sent tosampreeth77" + ack1[1]);
					ContentValues cv = new ContentValues();
					cv.put(KEY_FIELD_DB, ack1[0]);
					cv.put(VALUE_FIELD_DB, ack1[1]);
					cv.put(o_FIELD_DB, rep1portnumber);
					String se = DhtHelper.key + " like '" + selection + "'";
					db4.insertWithOnConflict(DhtHelper.TABLE4, null, cv,db4.CONFLICT_REPLACE);
					c2 = queryBuilder.query(db4, null, se, selectionArgs, null, null, null);
					if (c2.moveToFirst()) {
						Log.i("Insert Fwd", "Msg sent tosampreeth77" + c2.getString(0));
					}
				}
				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s1.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s1.close();
				}

			} catch (Exception e) {
				e.printStackTrace();

			}
			String msgTosend2 = selection + ":" + "Query";
			Socket s2 = null;
			try {
				queryBuilder.setTables(DhtHelper.TABLE5);
				SQLiteDatabase db5 = dbHelper.getWritableDatabase();
				s2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rep2portnumber));
				PrintWriter msgToServer = new PrintWriter(s2.getOutputStream(), true);
				msgToServer.write(msgTosend2);
				msgToServer.println();
				msgToServer.flush();
				Log.i("Insert Fwd", "Msg sent to88 " + rep2portnumber);

				BufferedReader readline = new BufferedReader(new InputStreamReader(s2.getInputStream()));
				String ack = "";
				ack = readline.readLine();
				Log.i("Insert Fwd", "Msg sent tosampreeth88" + ack);
				s2.close();

				//String [] ackp=ack.split("$");
				if(ack!=null) {
					String[] ack1 = ack.split("#");
					Log.i("Insert Fwd", "Msg sent tosampreeth88" + ack1[1]);
					ContentValues cv = new ContentValues();
					cv.put(KEY_FIELD_DB, ack1[0]);
					cv.put(VALUE_FIELD_DB, ack1[1]);
					cv.put(o_FIELD_DB, rep2portnumber);
					String se = DhtHelper.key + " like '" + selection + "'";
					db5.insertWithOnConflict(DhtHelper.TABLE5, null, cv,db5.CONFLICT_REPLACE);
					c3 = queryBuilder.query(db5, null, se, selectionArgs, null, null, null);

					if (c3.moveToFirst()) {
						Log.i("Insert Fwd", "Msg sent tosampreeth88" + c3.getString(0));
					}
				}
				if (ack == null)
				{
					Log.i(TAG, "Connection closed");
					s2.close();
				} else if (ack.contains("PA4 OK")) {
					Log.i(TAG, "AVD replied. Insert Ok");
					s2.close();
				}

			} catch (Exception e) {
				e.printStackTrace();

			}
			int c1g=0;
			if(c1==null){
				Log.i("Insert Fwd", "Msg sent tosampreeth99" + "c1 is null");
				c1g=0;
			}
			else{
				c1g=c1.getCount();
			}
			int c2g=0;
			if(c2==null){
				Log.i("Insert Fwd", "Msg sent tosampreeth99" + "c2 is null");
				c2g=0;
			}
			else{
				c2g=c2.getCount();
			}
			int c3g=0;
			if(c3==null){
				Log.i("Insert Fwd", "Msg sent tosampreeth99" + "c3 is null");
				c3g=0;
			}
			else{
				c3g=c3.getCount();
			}
			if(c1g==0)
			{

				cursor=c2;

			}
			else if(c2g==0)
			{

				cursor=c1;

			}
			else if(c3g==0)
			{

				cursor=c1;

			}
			else if(c1.getCount()>0 && c2.getCount()>0 && c3.getCount()>0)
			{
				String v1=c1.getString(2);
				String v2=c2.getString(2);
				String v3=c3.getString(2);
				ArrayList<String> vl=new ArrayList<String>();
				vl.add(v1);
				vl.add(v2);
				vl.add(v3);
				int occurrence1 = Collections.frequency(vl, v1);
				int occurrence2 = Collections.frequency(vl, v2);
				int occurrence3 = Collections.frequency(vl, v3);
				if(occurrence1>=2){
					cursor=c1;
				}
				else if(occurrence2>=2){
					cursor=c2;
				}
				else if(occurrence3>=2){
					cursor=c3;
				}
				else {
					cursor=c1;
				}




			}



		}

		// TODO Auto-generated method stub
		if(cursor.moveToFirst()){
			Log.i("Insert Fwd", "Msg sent tosampreeth444" + cursor.getString(0)+cursor.getString(1)+cursor.getString(2));

		}
		Log.i("Insert Fwd", "Msg sent tosampreeth333" + cursor.getCount());
		return cursor;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		SQLiteDatabase readableDatabase = dbHelper.getReadableDatabase();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(dbHelper.TABLE);
		String s1=dbHelper.key+" like '"+selection+"'";
		readableDatabase.update(dbHelper.TABLE, values, s1, null);

		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
