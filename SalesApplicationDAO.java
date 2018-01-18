package com.sales.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.sales.service.CustDetailsVO;
import com.sales.service.SalesVO;

public interface SalesApplicationDAO {
	
	public abstract void updateSalesDetails(SalesVO sVO);
	
	public abstract List <SalesVO> getCustomerDetails(String CustomerName);
	
	public abstract void updatePaymentDetails(String custName, Double amtRecevied);
	
	public abstract Map <Date, Double>  getPaymentDetails(String CustomerName);
	
	public abstract List<CustDetailsVO> getAllCustomerDetails();
	
}
