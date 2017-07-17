/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.playground.flink;

import java.util.Locale;

import org.apache.flink.api.java.tuple.Tuple20;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * A ChaseBankDeposit is a Deposit at a branch for Chase Bank. 
 * 
 * Institution name, main office, Branch Name, establishedDate, acquiredDate,
 * StreetAddress, city, county, state, zipcode, lat, long,
 * deposits2010,deposits2011, deposits2012, deposits2013, deposits2014,
 * deposits2015, deposits2016
 *
 */
public class ChaseBankDeposit {

	private static transient DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")
			.withLocale(Locale.US).withZoneUTC();

	public ChaseBankDeposit() {
	}

	// Institution name, main office, Branch Name, establishedDate,
	// acquiredDate, StreetAddress, city, county, state, zipcode,
	// lat, long, deposits2010,deposits2011, deposits2012, deposits2013,
	// deposits2014, deposits2015, deposits2016

	public String institutionName = "";
	public boolean isMainOffice = false;
	public String branchName = "";
	public int branchNumber = -1;
	public DateTime establishedDate;
	public DateTime acquiredDate;
	public String streetAddress = "";
	public String city = "";
	public String county = "";
	public String state = "";
	public String zipCode = "";
	public float longitude;
	public float latitude;
	public long deposits2010;
	public long deposits2011;
	public long deposits2012;
	public long deposits2013;
	public long deposits2014;
	public long deposits2015;
	public long deposits2016;

	public ChaseBankDeposit(String institutionName, boolean isMainOffice, String branchName, int branchNumber,
			DateTime establishedDate, DateTime acquiredDate, String streetAddress, String city, String county,
			String state, String zipCode, float longitude, float latitude, long deposits2010, long deposits2011,
			long deposits2012, long deposits2013, long deposits2014, long deposits2015, long deposits2016) {

		this.institutionName = institutionName;
		this.isMainOffice = isMainOffice;
		this.branchName = branchName;
		this.branchNumber = branchNumber;
		this.establishedDate = establishedDate;
		this.acquiredDate = acquiredDate;
		this.streetAddress = streetAddress;
		this.city = city;
		this.county = county;
		this.state = state;
		this.zipCode = zipCode;
		this.longitude = longitude;
		this.latitude = latitude;
		this.deposits2010 = deposits2010;
		this.deposits2011 = deposits2011;
		this.deposits2012 = deposits2012;
		this.deposits2013 = deposits2013;
		this.deposits2014 = deposits2014;
		this.deposits2015 = deposits2015;
		this.deposits2016 = deposits2016;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append(branchNumber).append(",");
		sb.append(isMainOffice ? "MAINOFFICE" : "BRANCH").append(",");
		sb.append(city).append(",");
		sb.append(state).append(",");
		sb.append(longitude).append(",");
		sb.append(deposits2010).append(",");
		sb.append(deposits2011).append(",");
		sb.append(deposits2012).append(",");
		sb.append(deposits2013).append(",");
		sb.append(deposits2014).append(",");
		sb.append(deposits2014).append(",");
		sb.append(deposits2016);
		sb.append("]");

		return sb.toString();
	}

	public static ChaseBankDeposit fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 20) {
			//throw new RuntimeException("Invalid record: " + line);
			return null;
		}

		ChaseBankDeposit ride = new ChaseBankDeposit();

		try {
			ride.institutionName = tokens[0];
			ride.isMainOffice = Integer.valueOf(tokens[1]) == 1 ? true : false;
			ride.branchName = tokens[2];
			ride.branchNumber = Integer.valueOf(tokens[3]);
			ride.establishedDate = DateTime.parse(tokens[4], timeFormatter);
			ride.acquiredDate = tokens[5] != null ? DateTime.parse(tokens[5], timeFormatter) : null;
			ride.streetAddress = tokens[6];
			ride.city = tokens[7];
			ride.county = tokens[8];
			ride.state = tokens[9];
			ride.zipCode = tokens[10];
			ride.latitude = tokens[11].length() > 0 ? Float.parseFloat(tokens[11]) : 0.0f;
			ride.longitude = tokens[12].length() > 0 ? Float.parseFloat(tokens[12]) : 0.0f;
			ride.deposits2010 = tokens[13].length() > 0 ? Long.parseLong(tokens[13]):0l;
			ride.deposits2011 = tokens[14].length() > 0 ?Long.parseLong(tokens[14]):0l;
			ride.deposits2012 = tokens[15].length() > 0 ?Long.parseLong(tokens[15]):0l;
			ride.deposits2013 = tokens[16].length() > 0 ?Long.parseLong(tokens[16]):0l;
			ride.deposits2014 = tokens[17].length() > 0 ?Long.parseLong(tokens[17]):0l;
			ride.deposits2015 = tokens[18].length() > 0 ?Long.parseLong(tokens[18]):0l;
			ride.deposits2016 = tokens[19].length() > 0 ? Long.parseLong(tokens[19]):0l;
		} catch (NumberFormatException nfe) {
			//throw new RuntimeException("Invalid record: " + line, nfe);
			return null;
		}
		catch (IllegalArgumentException iie) {
			return null;
		}

		return ride;
	}

	public static ChaseBankDeposit fromTuple(Tuple20<String, Boolean, String, Integer, String,String,
			String,String,String,String,String,Float,Float,
			Long,Long,Long,Long,Long,Long,Long> value) {

		ChaseBankDeposit ride = new ChaseBankDeposit();

		try {
			ride.institutionName = value.f0;
			ride.isMainOffice = value.f1;
			ride.branchName = value.f2;
			ride.branchNumber = value.f3;
			ride.establishedDate = DateTime.parse(value.f4, timeFormatter);
			ride.acquiredDate = value.f5 != null ? DateTime.parse(value.f5, timeFormatter) : null;
			ride.streetAddress = value.f6;
			ride.city = value.f7;
			ride.county = value.f8;
			ride.state = value.f9;
			ride.zipCode = value.f10;
			ride.latitude = value.f11;
			ride.longitude = value.f12;
			ride.deposits2010 = value.f13;
			ride.deposits2011 = value.f14;
			ride.deposits2012 = value.f15;
			ride.deposits2013 = value.f16;
			ride.deposits2014 = value.f17;
			ride.deposits2015 = value.f18;
			ride.deposits2016 = value.f19;
		} catch (NumberFormatException nfe) {
			return null;
		}
		catch (IllegalArgumentException iie) {
			return null;
		}

		return ride;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof ChaseBankDeposit &&
				this.city.equalsIgnoreCase(((ChaseBankDeposit) other).city);
	}

	@Override
	public int hashCode() {
		return (int)this.branchNumber;
	}
	
	public ChaseBankDeposit aggregateDeposits(ChaseBankDeposit arg1, ChaseBankDeposit arg2) {
		if(arg1 == arg2) {
			arg2.deposits2010 += arg1.deposits2010;
			arg2.deposits2011 += arg1.deposits2011;
			arg2.deposits2012 += arg1.deposits2012;
			arg2.deposits2013 += arg1.deposits2013;
			arg2.deposits2014 += arg1.deposits2014;
			arg2.deposits2015 += arg1.deposits2015;
			arg2.deposits2016 += arg1.deposits2016;
			return arg2;
		}
		else {
			System.out.println("City Deposits are not equal");
			return arg1;
		}
		
	}
	
}
