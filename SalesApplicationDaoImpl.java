package com.sales.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.fabric.xmlrpc.base.Array;
import com.sales.service.CustDetailsVO;
import com.sales.service.SalesVO;

public class SalesApplicationDaoImpl implements SalesApplicationDAO {

	@Override
	public void updateSalesDetails(SalesVO sVO) {
		// TODO Auto-generated method stub
	Connection connect = getConnection();
	String sql = "INSERT INTO CUST_SALES_TRANSACTION  (CUSTOMERID, CUSTOMER_NAME, BUSINESSTYPE, ITEMTYPE, QUANTITY, EACHPRICE, TOTALPRICE, TRANSACT_DATE) values (?,?,?,?,?,?,?,?)";
	if(connect!= null){
		
		try {
			PreparedStatement pstmt=connect.prepareStatement(sql);
			pstmt.setInt(1,sVO.getCustomerId());
			pstmt.setString(2,  sVO.getClientName().toUpperCase().trim());
			pstmt.setString(3, sVO.getBusinessType());
			pstmt.setString(4,sVO.getSelectType());
			pstmt.setLong(5,  sVO.getQuantity());
			pstmt.setDouble(6, sVO.getEachPrice());
			pstmt.setDouble(7,  sVO.getTotalPrice());
			pstmt.setTimestamp(8,  new java.sql.Timestamp(sVO.getDateOfPurchase().getTime()));
		
			int i=pstmt.executeUpdate();  
			System.out.println(i+" records inserted");  
			  
			connect.close();  
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	else{
		System.out.println(" Could Not Establish Connection for updateSalesDetails"); 
	}
		
	}

	@Override
	public List <SalesVO> getCustomerDetails(String CustomerName) {
		// TODO Auto-generated method stub
		
		List <SalesVO> listVO = new ArrayList <SalesVO>();
		
		Connection connect = getConnection();
		String sql = "SELECT CUSTOMER_NAME, BUSINESSTYPE, ITEMTYPE, QUANTITY, EACHPRICE, TOTALPRICE,TRANSACT_DATE from CUST_SALES_TRANSACTION where CUSTOMER_NAME = ? ";
		if(connect!= null){
			
			try {
				PreparedStatement pstmt=connect.prepareStatement(sql);
				pstmt.setString(1,CustomerName.toUpperCase());
				

				ResultSet rs =pstmt.executeQuery();  
				
				while (rs.next()){
					
					SalesVO sVO = new SalesVO();
					sVO.setClientName(rs.getString("CUSTOMER_NAME").toUpperCase().trim());
					sVO.setBusinessType(rs.getString("BUSINESSTYPE"));
					sVO.setSelectType(rs.getString("ITEMTYPE"));
					sVO.setQuantity(rs.getLong("QUANTITY"));
					sVO.setEachPrice(rs.getDouble("EACHPRICE"));
					sVO.setTotalPrice(rs.getDouble("TOTALPRICE"));
					sVO.setDateOfPurchase(rs.getTimestamp("TRANSACT_DATE"));
					listVO.add(sVO);
				}
				  
				connect.close();  
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else{
			System.out.println(" Could Not Establish Connection for getCustomerDetails"); 
		}
		
		return listVO;
	}
	
	public Connection getConnection()  {
		Connection con = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			 con=DriverManager.getConnection(  
					"jdbc:mysql://localhost:3306/salesapplication","root","root");  
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		return con;
	}

	@Override
	public void updatePaymentDetails(String custName, Double amtRecevied) {
		// TODO Auto-generated method stub
		
		Connection connect = getConnection();
		String sql = "INSERT INTO CUST_PAYMENT_TRANSACTION  ( CUSTOMERID, CUSTOMER_NAME, AMNTRECEIVED, TRANSACT_DATE) values (?,?,?,?)";
		if(connect!= null){
			
			try {
				PreparedStatement pstmt=connect.prepareStatement(sql);
				pstmt.setInt(1,0);
				pstmt.setString(2,  custName.toUpperCase().trim());
				pstmt.setDouble(3, amtRecevied);
				pstmt.setTimestamp(4,  new java.sql.Timestamp(new Date().getTime()));
			
				int i=pstmt.executeUpdate();  
				System.out.println(i+" records inserted");  
				  
				connect.close();  
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		else{
			System.out.println(" Could Not Establish Connection for updateSalesDetails"); 
		}
			
	}

	@Override
	public Map <Date, Double>  getPaymentDetails(String CustomerName) {

		// TODO Auto-generated method stub
		
		Map <Date, Double> map = new HashMap<Date, Double>();
		
		Connection connect = getConnection();
		String sql = "SELECT CUSTOMER_NAME,  AMNTRECEIVED, TRANSACT_DATE from CUST_PAYMENT_TRANSACTION where CUSTOMER_NAME = ? ";
		if(connect!= null){
			
			try {
				PreparedStatement pstmt=connect.prepareStatement(sql);
				pstmt.setString(1,CustomerName.toUpperCase().trim());
				

				ResultSet rs =pstmt.executeQuery();  
				
				while (rs.next()){
					
					map.put(rs.getTimestamp("TRANSACT_DATE"), rs.getDouble("AMNTRECEIVED"));
					
				}
				  
				connect.close();  
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else{
			System.out.println(" Could Not Establish Connection for getCustomerDetails"); 
		}

		Map <Date, Double> mapSorted = new TreeMap<Date, Double>(map);
		
		return mapSorted;
	}

	@Override
	public List<CustDetailsVO> getAllCustomerDetails() {
		List <CustDetailsVO> custList = new ArrayList<CustDetailsVO>();
		
			String queryPayment = "select CUSTOMER_NAME, SUM(AMNTRECEIVED) as TOTALPAYMNTRECV from  CUST_PAYMENT_TRANSACTION Group by Customer_Name";

			String querySales = "select CUSTOMER_NAME,  SUM(TOTALPRICE)  as TOTALSALEPRICE from CUST_SALES_TRANSACTION Group by Customer_Name";
			Map<String, Double> mapCustPaymnt = new HashMap<String, Double>();
			
			Connection connect = getConnection();
			if(connect!= null){
				
				try {
					
					Statement stmt=connect.createStatement();
					ResultSet rs =stmt.executeQuery(queryPayment);  
					
					while (rs.next()){
						
						mapCustPaymnt.put(rs.getString("CUSTOMER_NAME").toUpperCase().trim(), rs.getDouble("TOTALPAYMNTRECV"));
						
					}
				
					Statement stmtSales=connect.createStatement();
					ResultSet rsSales =stmtSales.executeQuery(querySales);  
					
					while (rsSales.next()){
						
						String cName = rsSales.getString("CUSTOMER_NAME").toUpperCase().trim();
						Double totPrice = rsSales.getDouble("TOTALSALEPRICE");
						
						Double totPayRecv = mapCustPaymnt.get(cName);
						
						CustDetailsVO custDetailVO   = new CustDetailsVO();
						
						custDetailVO.setCustomerName(cName);
						if(totPayRecv ==null)
							custDetailVO.setTotalPaymentReceived(0.0);
						else
							custDetailVO.setTotalPaymentReceived(totPayRecv);	
						if(totPrice ==null)
							custDetailVO.setTotalSalesPrice(0.0);
						else
							custDetailVO.setTotalSalesPrice(totPrice);
						custList.add(custDetailVO);
					}
				connect.close();  
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else{
			System.out.println(" Could Not Establish Connection for getALlCustomerDetails"); 
		}
		return custList;
	}

}
