from fastapi import APIRouter
from fastapi import Body
from fastapi.responses import Response
from io import BytesIO
from datetime import datetime

from utils.xbrl_utils import make_minimal_instance, make_ixbrl_html
from utils.pdf_utils import generate_pdf_report
from routes.route_utils import generate_excel_report


router = APIRouter(prefix="/xbrl", tags=["xbrl"])


@router.post("/generate")
async def generate_xbrl(payload: dict = Body(...)):
	entity_lei = payload.get('entityLei', '00000000000000000000')
	start = payload.get('startDate', '2025-01-01')
	end = payload.get('endDate', '2025-03-31')
	currency = payload.get('currency', 'EGP')
	facts = payload.get('facts', { 'ifrs-full:Revenue': 123456 })

	content = make_minimal_instance(entity_lei, start, end, currency, facts)
	return Response(
		content=content,
		media_type='application/xml',
		headers={
			'Content-Disposition': 'attachment; filename="report_instance.xbrl"',
			'Content-Length': str(len(content)),
			'Cache-Control': 'no-cache, no-store, must-revalidate'
		},
		status_code=200
	)


@router.post("/generate-ixbrl")
async def generate_ixbrl(payload: dict = Body(...)):
	"""Generate iXBRL (Inline XBRL) HTML document"""
	try:
		entity_lei = payload.get('entityLei', '00000000000000000000')
		entity_name = payload.get('entityName', 'Company')
		entity_name_ar = payload.get('entityNameAr', '')
		start = payload.get('startDate', '2025-01-01')
		end = payload.get('endDate', '2025-03-31')
		currency = payload.get('currency', 'EGP')
		
		# Build facts from financial data
		facts = {}
		if 'facts' in payload:
			facts = payload['facts']
		else:
			# Build facts from financial data structure
			assets_current = payload.get('assetsCurrent', 0)
			assets_noncurrent = payload.get('assetsNoncurrent', 0)
			liabilities_current = payload.get('liabilitiesCurrent', 0)
			liabilities_noncurrent = payload.get('liabilitiesNoncurrent', 0)
			equity = payload.get('equity', 0)
			revenue = payload.get('revenue', 0)
			cost_of_sales = payload.get('costOfSales', 0)
			operating_expenses = payload.get('operatingExpenses', 0)
			finance_costs = payload.get('financeCosts', 0)
			finance_income = payload.get('financeIncome', 0)
			tax_expense = payload.get('taxExpense', 0)
			cash_from_operations = payload.get('cashFromOperations', 0)
			cash_from_investing = payload.get('cashFromInvesting', 0)
			cash_from_financing = payload.get('cashFromFinancing', 0)
			
			facts = {
				'ifrs-full:AssetsCurrent': assets_current,
				'ifrs-full:AssetsNoncurrent': assets_noncurrent,
				'ifrs-full:Assets': assets_current + assets_noncurrent,
				'ifrs-full:LiabilitiesCurrent': liabilities_current,
				'ifrs-full:LiabilitiesNoncurrent': liabilities_noncurrent,
				'ifrs-full:Liabilities': liabilities_current + liabilities_noncurrent,
				'ifrs-full:Equity': equity,
				'ifrs-full:Revenue': revenue,
				'ifrs-full:CostOfSales': cost_of_sales,
				'ifrs-full:GrossProfit': revenue - cost_of_sales,
				'ifrs-full:OperatingExpenses': operating_expenses,
				'ifrs-full:FinanceCosts': finance_costs,
				'ifrs-full:FinanceIncome': finance_income,
				'ifrs-full:IncomeTaxExpense': tax_expense,
				'ifrs-full:ProfitLoss': revenue - cost_of_sales - operating_expenses - finance_costs + finance_income - tax_expense,
				'ifrs-full:CashGeneratedFromOperations': cash_from_operations,
				'ifrs-full:CashFlowsFromUsedInInvestingActivities': cash_from_investing,
				'ifrs-full:CashFlowsFromUsedInFinancingActivities': cash_from_financing,
				'ifrs-full:CashAndCashEquivalentsAtEndOfPeriod': cash_from_operations + cash_from_investing + cash_from_financing
			}
		
		html_content = make_ixbrl_html(entity_lei, entity_name, entity_name_ar, start, end, currency, facts)
		
		return Response(
			content=html_content,
			media_type='text/html',
			headers={
				'Content-Disposition': f'attachment; filename="financial_report_{end}.html"',
				'Content-Length': str(len(html_content)),
				'Cache-Control': 'no-cache, no-store, must-revalidate'
			},
			status_code=200
		)
	except Exception as e:
		return Response(
			f'Error generating iXBRL: {str(e)}',
			status_code=500,
			media_type='text/plain'
		)


@router.post("/generate-pdf")
async def generate_xbrl_pdf(payload: dict = Body(...)):
	"""Generate PDF from financial data"""
	import traceback
	import logging
	logger = logging.getLogger(__name__)
	
	try:
		logger.info(f"PDF generation request received. Entity: {payload.get('entityName', 'Unknown')}")
		entity_name = payload.get('entityName', 'Company')
		entity_name_ar = payload.get('entityNameAr', '')
		start = payload.get('startDate', '2025-01-01')
		end = payload.get('endDate', '2025-03-31')
		currency = payload.get('currency', 'EGP')
		
		# Build financial statements data
		columns = [
			'Line Item',
			'Amount',
			'Prior Period'
		]
		
		data_rows = []
		
		# Balance Sheet
		data_rows.append(['ASSETS', '', ''])
		data_rows.append(['Current Assets', f"{payload.get('assetsCurrent', 0):,.0f}", f"{payload.get('assetsCurrentPrior', 0):,.0f}"])
		data_rows.append(['Non-current Assets', f"{payload.get('assetsNoncurrent', 0):,.0f}", f"{payload.get('assetsNoncurrentPrior', 0):,.0f}"])
		data_rows.append(['Total Assets', f"{payload.get('assetsCurrent', 0) + payload.get('assetsNoncurrent', 0):,.0f}", f"{payload.get('assetsCurrentPrior', 0) + payload.get('assetsNoncurrentPrior', 0):,.0f}"])
		data_rows.append(['', '', ''])
		data_rows.append(['LIABILITIES', '', ''])
		data_rows.append(['Current Liabilities', f"{payload.get('liabilitiesCurrent', 0):,.0f}", f"{payload.get('liabilitiesCurrentPrior', 0):,.0f}"])
		data_rows.append(['Non-current Liabilities', f"{payload.get('liabilitiesNoncurrent', 0):,.0f}", f"{payload.get('liabilitiesNoncurrentPrior', 0):,.0f}"])
		data_rows.append(['Total Liabilities', f"{payload.get('liabilitiesCurrent', 0) + payload.get('liabilitiesNoncurrent', 0):,.0f}", f"{payload.get('liabilitiesCurrentPrior', 0) + payload.get('liabilitiesNoncurrentPrior', 0):,.0f}"])
		data_rows.append(['Equity', f"{payload.get('equity', 0):,.0f}", f"{payload.get('equityPrior', 0):,.0f}"])
		data_rows.append(['', '', ''])
		
		# Income Statement
		data_rows.append(['INCOME STATEMENT', '', ''])
		revenue = payload.get('revenue', 0)
		cost_of_sales = payload.get('costOfSales', 0)
		operating_expenses = payload.get('operatingExpenses', 0)
		finance_costs = payload.get('financeCosts', 0)
		finance_income = payload.get('financeIncome', 0)
		tax_expense = payload.get('taxExpense', 0)
		
		data_rows.append(['Revenue', f"{revenue:,.0f}", f"{payload.get('revenuePrior', 0):,.0f}"])
		data_rows.append(['Cost of Sales', f"({cost_of_sales:,.0f})", f"({payload.get('costOfSalesPrior', 0):,.0f})"])
		data_rows.append(['Gross Profit', f"{revenue - cost_of_sales:,.0f}", f"{payload.get('revenuePrior', 0) - payload.get('costOfSalesPrior', 0):,.0f}"])
		data_rows.append(['Operating Expenses', f"({operating_expenses:,.0f})", f"({payload.get('operatingExpensesPrior', 0):,.0f})"])
		data_rows.append(['Finance Costs', f"({finance_costs:,.0f})", ''])
		data_rows.append(['Finance Income', f"{finance_income:,.0f}", ''])
		data_rows.append(['Tax Expense', f"({tax_expense:,.0f})", ''])
		net_income = revenue - cost_of_sales - operating_expenses - finance_costs + finance_income - tax_expense
		data_rows.append(['Net Income', f"{net_income:,.0f}", ''])
		
		# Cash Flow
		data_rows.append(['', '', ''])
		data_rows.append(['CASH FLOW STATEMENT', '', ''])
		data_rows.append(['Cash from Operations', f"{payload.get('cashFromOperations', 0):,.0f}", ''])
		data_rows.append(['Cash from Investing', f"{payload.get('cashFromInvesting', 0):,.0f}", ''])
		data_rows.append(['Cash from Financing', f"{payload.get('cashFromFinancing', 0):,.0f}", ''])
		net_cash = payload.get('cashFromOperations', 0) + payload.get('cashFromInvesting', 0) + payload.get('cashFromFinancing', 0)
		data_rows.append(['Net Change in Cash', f"{net_cash:,.0f}", ''])
		
		# Header config
		header_config = {
			'includeHeader': True,
			'title': f'{entity_name} - Quarterly Financial Report',
			'subtitle': f'Period: {start} to {end}',
			'showDate': True,
			'showBankInfo': True,
			'bankName': entity_name,
			'currency': currency,
			'tableHeaderBgColor': '#1F4E79',
			'tableBodyBgColor': '#FFFFFF',
		}
		
		logger.info("Generating PDF content...")
		pdf_content = generate_pdf_report(columns, data_rows, header_config)
		logger.info(f"PDF generated successfully. Size: {len(pdf_content)} bytes")
		
		# Set headers to prevent IDM interception and ensure proper download
		# Use filename* with UTF-8 encoding to prevent IDM from intercepting
		filename = f"financial_report_{end}.pdf"
		headers = {
			'Content-Disposition': f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}',
			'Content-Type': 'application/pdf',
			'Content-Length': str(len(pdf_content)),
			'Cache-Control': 'no-cache, no-store, must-revalidate, private',
			'Pragma': 'no-cache',
			'Expires': '0',
			'X-Content-Type-Options': 'nosniff',
			'Accept-Ranges': 'bytes',
			'Content-Transfer-Encoding': 'binary',
			'X-Download-Options': 'noopen',
			'X-Requested-With': 'XMLHttpRequest'
		}
		
		logger.info("Returning PDF response with status 200")
		return Response(
			content=pdf_content,
			media_type='application/pdf',
			headers=headers,
			status_code=200
		)
	except Exception as e:
		error_msg = str(e)
		traceback_str = traceback.format_exc()
		print(f"ERROR in generate_xbrl_pdf: {error_msg}")
		print(traceback_str)
		return Response(
			f'Error generating PDF: {error_msg}\n\nTraceback:\n{traceback_str}',
			status_code=500,
			media_type='text/plain'
		)


@router.post("/generate-excel")
async def generate_xbrl_excel(payload: dict = Body(...)):
	"""Generate Excel from financial data"""
	import traceback
	import logging
	logger = logging.getLogger(__name__)
	
	try:
		logger.info(f"Excel generation request received. Entity: {payload.get('entityName', 'Unknown')}")
		entity_name = payload.get('entityName', 'Company')
		entity_name_ar = payload.get('entityNameAr', '')
		start = payload.get('startDate', '2025-01-01')
		end = payload.get('endDate', '2025-03-31')
		currency = payload.get('currency', 'EGP')
		
		# Build financial statements data
		columns = [
			'Line Item',
			'Amount',
			'Prior Period'
		]
		
		data_rows = []
		
		# Balance Sheet - ensure all values are numbers
		data_rows.append(['ASSETS', '', ''])
		assets_current = float(payload.get('assetsCurrent', 0) or 0)
		assets_noncurrent = float(payload.get('assetsNoncurrent', 0) or 0)
		assets_current_prior = float(payload.get('assetsCurrentPrior', 0) or 0)
		assets_noncurrent_prior = float(payload.get('assetsNoncurrentPrior', 0) or 0)
		
		data_rows.append(['Current Assets', assets_current, assets_current_prior])
		data_rows.append(['Non-current Assets', assets_noncurrent, assets_noncurrent_prior])
		data_rows.append(['Total Assets', assets_current + assets_noncurrent, assets_current_prior + assets_noncurrent_prior])
		data_rows.append(['', '', ''])
		data_rows.append(['LIABILITIES', '', ''])
		
		liabilities_current = float(payload.get('liabilitiesCurrent', 0) or 0)
		liabilities_noncurrent = float(payload.get('liabilitiesNoncurrent', 0) or 0)
		liabilities_current_prior = float(payload.get('liabilitiesCurrentPrior', 0) or 0)
		liabilities_noncurrent_prior = float(payload.get('liabilitiesNoncurrentPrior', 0) or 0)
		equity = float(payload.get('equity', 0) or 0)
		equity_prior = float(payload.get('equityPrior', 0) or 0)
		
		data_rows.append(['Current Liabilities', liabilities_current, liabilities_current_prior])
		data_rows.append(['Non-current Liabilities', liabilities_noncurrent, liabilities_noncurrent_prior])
		data_rows.append(['Total Liabilities', liabilities_current + liabilities_noncurrent, liabilities_current_prior + liabilities_noncurrent_prior])
		data_rows.append(['Equity', equity, equity_prior])
		data_rows.append(['', '', ''])
		
		# Income Statement
		data_rows.append(['INCOME STATEMENT', '', ''])
		revenue = float(payload.get('revenue', 0) or 0)
		cost_of_sales = float(payload.get('costOfSales', 0) or 0)
		operating_expenses = float(payload.get('operatingExpenses', 0) or 0)
		finance_costs = float(payload.get('financeCosts', 0) or 0)
		finance_income = float(payload.get('financeIncome', 0) or 0)
		tax_expense = float(payload.get('taxExpense', 0) or 0)
		revenue_prior = float(payload.get('revenuePrior', 0) or 0)
		cost_of_sales_prior = float(payload.get('costOfSalesPrior', 0) or 0)
		operating_expenses_prior = float(payload.get('operatingExpensesPrior', 0) or 0)
		
		data_rows.append(['Revenue', revenue, revenue_prior])
		data_rows.append(['Cost of Sales', -cost_of_sales, -cost_of_sales_prior])
		data_rows.append(['Gross Profit', revenue - cost_of_sales, revenue_prior - cost_of_sales_prior])
		data_rows.append(['Operating Expenses', -operating_expenses, -operating_expenses_prior])
		data_rows.append(['Finance Costs', -finance_costs, 0])
		data_rows.append(['Finance Income', finance_income, 0])
		data_rows.append(['Tax Expense', -tax_expense, 0])
		net_income = revenue - cost_of_sales - operating_expenses - finance_costs + finance_income - tax_expense
		data_rows.append(['Net Income', net_income, 0])
		
		# Cash Flow
		data_rows.append(['', '', ''])
		data_rows.append(['CASH FLOW STATEMENT', '', ''])
		cash_from_operations = float(payload.get('cashFromOperations', 0) or 0)
		cash_from_investing = float(payload.get('cashFromInvesting', 0) or 0)
		cash_from_financing = float(payload.get('cashFromFinancing', 0) or 0)
		
		data_rows.append(['Cash from Operations', cash_from_operations, 0])
		data_rows.append(['Cash from Investing', cash_from_investing, 0])
		data_rows.append(['Cash from Financing', cash_from_financing, 0])
		net_cash = cash_from_operations + cash_from_investing + cash_from_financing
		data_rows.append(['Net Change in Cash', net_cash, 0])
		
		# Header config
		header_config = {
			'includeHeader': True,
			'title': f'{entity_name} - Quarterly Financial Report',
			'subtitle': f'Period: {start} to {end}',
			'showDate': True,
			'showBankInfo': True,
			'bankName': entity_name,
			'currency': currency,
			'tableHeaderBgColor': '#1F4E79',
			'tableBodyBgColor': '#FFFFFF',
		}
		
		logger.info("Generating Excel content...")
		# generate_excel_report from route_utils expects data_rows as list of lists
		excel_content = generate_excel_report(columns, data_rows, header_config)
		logger.info(f"Excel generated successfully. Size: {len(excel_content)} bytes")
		
		return Response(
			content=excel_content,
			media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
			headers={
				'Content-Disposition': f'attachment; filename="financial_report_{end}.xlsx"',
				'Content-Length': str(len(excel_content)),
				'Cache-Control': 'no-cache, no-store, must-revalidate',
				'X-Content-Type-Options': 'nosniff'
			},
			status_code=200
		)
	except Exception as e:
		error_msg = str(e)
		traceback_str = traceback.format_exc()
		logger.error(f"ERROR in generate_xbrl_excel: {error_msg}")
		logger.error(traceback_str)
		print(f"ERROR in generate_xbrl_excel: {error_msg}")
		print(traceback_str)
		return Response(
			f'Error generating Excel: {error_msg}\n\nTraceback:\n{traceback_str}',
			status_code=500,
			media_type='text/plain'
		)



@router.post("/generate-word")
async def generate_xbrl_word(payload: dict = Body(...)):
	"""Generate Word document from financial data"""
	import traceback
	import logging
	from routes.route_utils import generate_word_report
	
	logger = logging.getLogger(__name__)
	
	try:
		logger.info(f"Word generation request received. Entity: {payload.get('entityName', 'Unknown')}")
		entity_name = payload.get('entityName', 'Company')
		entity_name_ar = payload.get('entityNameAr', '')
		start = payload.get('startDate', '2025-01-01')
		end = payload.get('endDate', '2025-03-31')
		currency = payload.get('currency', 'EGP')
		
		# Build financial statements data
		columns = [
			'Line Item',
			'Amount',
			'Prior Period'
		]
		
		data_rows = []
		
		# Balance Sheet
		data_rows.append(['ASSETS', '', ''])
		assets_current = float(payload.get('assetsCurrent', 0) or 0)
		assets_noncurrent = float(payload.get('assetsNoncurrent', 0) or 0)
		assets_current_prior = float(payload.get('assetsCurrentPrior', 0) or 0)
		assets_noncurrent_prior = float(payload.get('assetsNoncurrentPrior', 0) or 0)
		
		data_rows.append(['Current Assets', assets_current, assets_current_prior])
		data_rows.append(['Non-current Assets', assets_noncurrent, assets_noncurrent_prior])
		data_rows.append(['Total Assets', assets_current + assets_noncurrent, assets_current_prior + assets_noncurrent_prior])
		data_rows.append(['', '', ''])
		data_rows.append(['LIABILITIES', '', ''])
		
		liabilities_current = float(payload.get('liabilitiesCurrent', 0) or 0)
		liabilities_noncurrent = float(payload.get('liabilitiesNoncurrent', 0) or 0)
		liabilities_current_prior = float(payload.get('liabilitiesCurrentPrior', 0) or 0)
		liabilities_noncurrent_prior = float(payload.get('liabilitiesNoncurrentPrior', 0) or 0)
		equity = float(payload.get('equity', 0) or 0)
		equity_prior = float(payload.get('equityPrior', 0) or 0)
		
		data_rows.append(['Current Liabilities', liabilities_current, liabilities_current_prior])
		data_rows.append(['Non-current Liabilities', liabilities_noncurrent, liabilities_noncurrent_prior])
		data_rows.append(['Total Liabilities', liabilities_current + liabilities_noncurrent, liabilities_current_prior + liabilities_noncurrent_prior])
		data_rows.append(['Equity', equity, equity_prior])
		data_rows.append(['', '', ''])
		
		# Income Statement
		data_rows.append(['INCOME STATEMENT', '', ''])
		revenue = float(payload.get('revenue', 0) or 0)
		cost_of_sales = float(payload.get('costOfSales', 0) or 0)
		operating_expenses = float(payload.get('operatingExpenses', 0) or 0)
		finance_costs = float(payload.get('financeCosts', 0) or 0)
		finance_income = float(payload.get('financeIncome', 0) or 0)
		tax_expense = float(payload.get('taxExpense', 0) or 0)
		revenue_prior = float(payload.get('revenuePrior', 0) or 0)
		cost_of_sales_prior = float(payload.get('costOfSalesPrior', 0) or 0)
		operating_expenses_prior = float(payload.get('operatingExpensesPrior', 0) or 0)
		
		data_rows.append(['Revenue', revenue, revenue_prior])
		data_rows.append(['Cost of Sales', -cost_of_sales, -cost_of_sales_prior])
		data_rows.append(['Gross Profit', revenue - cost_of_sales, revenue_prior - cost_of_sales_prior])
		data_rows.append(['Operating Expenses', -operating_expenses, -operating_expenses_prior])
		data_rows.append(['Finance Costs', -finance_costs, 0])
		data_rows.append(['Finance Income', finance_income, 0])
		data_rows.append(['Tax Expense', -tax_expense, 0])
		net_income = revenue - cost_of_sales - operating_expenses - finance_costs + finance_income - tax_expense
		data_rows.append(['Net Income', net_income, 0])
		
		# Cash Flow
		data_rows.append(['', '', ''])
		data_rows.append(['CASH FLOW STATEMENT', '', ''])
		cash_from_operations = float(payload.get('cashFromOperations', 0) or 0)
		cash_from_investing = float(payload.get('cashFromInvesting', 0) or 0)
		cash_from_financing = float(payload.get('cashFromFinancing', 0) or 0)
		
		data_rows.append(['Cash from Operations', cash_from_operations, 0])
		data_rows.append(['Cash from Investing', cash_from_investing, 0])
		data_rows.append(['Cash from Financing', cash_from_financing, 0])
		net_cash = cash_from_operations + cash_from_investing + cash_from_financing
		data_rows.append(['Net Change in Cash', net_cash, 0])
		
		# Header config
		header_config = {
			'includeHeader': True,
			'title': f'{entity_name} - Quarterly Financial Report',
			'subtitle': f'Period: {start} to {end}',
			'showDate': True,
			'showBankInfo': True,
			'bankName': entity_name,
			'currency': currency,
			'tableHeaderBgColor': '#1F4E79',
			'tableBodyBgColor': '#FFFFFF',
		}
		
		logger.info("Generating Word content...")
		word_content = generate_word_report(columns, data_rows, header_config)
		logger.info(f"Word generated successfully. Size: {len(word_content)} bytes")
		
		return Response(
			content=word_content,
			media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
			headers={
				'Content-Disposition': f'attachment; filename="financial_report_{end}.docx"',
				'Content-Length': str(len(word_content)),
				'Cache-Control': 'no-cache, no-store, must-revalidate',
				'X-Content-Type-Options': 'nosniff'
			},
			status_code=200
		)
	except Exception as e:
		error_msg = str(e)
		traceback_str = traceback.format_exc()
		logger.error(f"ERROR in generate_xbrl_word: {error_msg}")
		logger.error(traceback_str)
		print(f"ERROR in generate_xbrl_word: {error_msg}")
		print(traceback_str)
		return Response(
			f'Error generating Word: {error_msg}\n\nTraceback:\n{traceback_str}',
			status_code=500,
			media_type='text/plain'
		)



