from lxml import etree


NS = {
	'xbrli': 'http://www.xbrl.org/2003/instance',
	'link': 'http://www.xbrl.org/2003/linkbase',
	'xlink': 'http://www.w3.org/1999/xlink',
	'iso4217': 'http://www.xbrl.org/2003/iso4217',
	'ifrs-full': 'http://xbrl.ifrs.org/taxonomy/2019-03-27/ifrs-full',
}


def make_minimal_instance(entity_lei: str, start: str, end: str, currency: str, facts: dict) -> bytes:
	"""
	Generate XBRL instance with instant and duration contexts.
	
	Args:
		entity_lei: Legal Entity Identifier
		start: Period start date (YYYY-MM-DD)
		end: Period end date (YYYY-MM-DD)
		currency: Currency code (e.g., 'EGP', 'USD')
		facts: Dictionary of concept QNames to values
			- Balance sheet items use instant context (end date)
			- Income statement and cash flow use duration context (start-end)
	
	Returns:
		XBRL instance as bytes
	"""
	root = etree.Element('{%s}xbrl' % NS['xbrli'], nsmap=NS)

	# schemaRef placeholder (replace href with real entrypoint when ready)
	schema_ref = etree.SubElement(root, '{%s}schemaRef' % NS['link'])
	schema_ref.set('{%s}type' % NS['xlink'], 'simple')
	schema_ref.set('{%s}href' % NS['xlink'], 'ifrs-full_entry_point.xsd')

	# Context 1: Instant (for Balance Sheet items)
	ctx_instant = etree.SubElement(root, '{%s}context' % NS['xbrli'], id='C_Instant')
	entity_inst = etree.SubElement(ctx_instant, '{%s}entity' % NS['xbrli'])
	identifier_inst = etree.SubElement(entity_inst, '{%s}identifier' % NS['xbrli'], scheme='http://standards.iso.org/iso/17442')
	identifier_inst.text = entity_lei
	period_inst = etree.SubElement(ctx_instant, '{%s}period' % NS['xbrli'])
	instant_date = etree.SubElement(period_inst, '{%s}instant' % NS['xbrli'])
	instant_date.text = end  # Balance sheet is at end date

	# Context 2: Duration (for Income Statement and Cash Flow)
	ctx_duration = etree.SubElement(root, '{%s}context' % NS['xbrli'], id='C_Duration')
	entity_dur = etree.SubElement(ctx_duration, '{%s}entity' % NS['xbrli'])
	identifier_dur = etree.SubElement(entity_dur, '{%s}identifier' % NS['xbrli'], scheme='http://standards.iso.org/iso/17442')
	identifier_dur.text = entity_lei
	period_dur = etree.SubElement(ctx_duration, '{%s}period' % NS['xbrli'])
	start_date = etree.SubElement(period_dur, '{%s}startDate' % NS['xbrli'])
	start_date.text = start
	end_date = etree.SubElement(period_dur, '{%s}endDate' % NS['xbrli'])
	end_date.text = end

	# Unit (currency)
	unit = etree.SubElement(root, '{%s}unit' % NS['xbrli'], id='U_currency')
	measure = etree.SubElement(unit, '{%s}measure' % NS['xbrli'])
	measure.text = f'iso4217:{currency}'

	# Balance sheet items (instant context) - typically end with Assets, Liabilities, Equity
	instant_concepts = [
		'Assets', 'AssetsCurrent', 'AssetsNoncurrent',
		'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
		'Equity', 'CashAndCashEquivalents'
	]

	# Facts mapping - determine context based on concept name
	for qname, value in facts.items():
		prefix, local = qname.split(':', 1) if ':' in qname else ('ifrs-full', qname)
		ns = NS.get(prefix)
		if not ns:
			continue
		
		# Determine context: instant for balance sheet, duration for P&L and cash flow
		is_instant = any(instant in local for instant in instant_concepts)
		context_ref = 'C_Instant' if is_instant else 'C_Duration'
		
		fact = etree.SubElement(root, f'{{{ns}}}{local}', contextRef=context_ref, unitRef='U_currency', decimals='-3')
		fact.text = str(value)

	return etree.tostring(root, xml_declaration=True, encoding='UTF-8', pretty_print=True)


def make_ixbrl_html(entity_lei: str, entity_name: str, entity_name_ar: str, start: str, end: str, currency: str, facts: dict) -> bytes:
	"""
	Generate iXBRL (Inline XBRL) HTML document with embedded XBRL tags.
	
	Args:
		entity_lei: Legal Entity Identifier
		entity_name: Company name in English
		entity_name_ar: Company name in Arabic
		start: Period start date (YYYY-MM-DD)
		end: Period end date (YYYY-MM-DD)
		currency: Currency code (e.g., 'EGP', 'USD')
		facts: Dictionary of concept QNames to values
	
	Returns:
		iXBRL HTML as bytes
	"""
	from datetime import datetime
	
	# Format currency
	currency_symbol = {'EGP': 'EGP', 'USD': '$', 'EUR': '€'}.get(currency, currency)
	
	# Determine instant vs duration concepts
	instant_concepts = [
		'Assets', 'AssetsCurrent', 'AssetsNoncurrent',
		'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
		'Equity', 'CashAndCashEquivalents'
	]
	
	def format_value(value):
		"""Format numeric value with thousand separators"""
		try:
			return f"{float(value):,.0f}"
		except:
			return str(value)
	
	def get_context_ref(qname):
		"""Determine context reference for a concept"""
		_, local = qname.split(':', 1) if ':' in qname else ('ifrs-full', qname)
		is_instant = any(instant in local for instant in instant_concepts)
		return 'C_Instant' if is_instant else 'C_Duration'
	
	def get_concept_name(qname):
		"""Get human-readable name from concept QName"""
		_, local = qname.split(':', 1) if ':' in qname else ('ifrs-full', qname)
		# Convert camelCase to Title Case
		import re
		return re.sub(r'([a-z])([A-Z])', r'\1 \2', local).title()
	
	# Build HTML
	html = f"""<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:iso4217="http://www.xbrl.org/2003/iso4217" xmlns:ifrs-full="http://xbrl.ifrs.org/taxonomy/2019-03-27/ifrs-full">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>{entity_name} - Quarterly Financial Report</title>
	<style>
		body {{
			font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
			margin: 0;
			padding: 20px;
			background-color: #f5f5f5;
			color: #333;
		}}
		.container {{
			max-width: 1200px;
			margin: 0 auto;
			background: white;
			padding: 40px;
			box-shadow: 0 2px 10px rgba(0,0,0,0.1);
		}}
		.header {{
			text-align: center;
			border-bottom: 3px solid #1F4E79;
			padding-bottom: 20px;
			margin-bottom: 30px;
		}}
		.header h1 {{
			color: #1F4E79;
			margin: 0;
			font-size: 28px;
		}}
		.header h2 {{
			color: #666;
			margin: 10px 0;
			font-size: 18px;
			font-weight: normal;
		}}
		.meta-info {{
			text-align: center;
			color: #666;
			margin-bottom: 30px;
			font-size: 14px;
		}}
		.statement {{
			margin-bottom: 40px;
		}}
		.statement h3 {{
			color: #1F4E79;
			border-bottom: 2px solid #1F4E79;
			padding-bottom: 10px;
			margin-bottom: 20px;
		}}
		table {{
			width: 100%;
			border-collapse: collapse;
			margin-bottom: 20px;
		}}
		th {{
			background-color: #1F4E79;
			color: white;
			padding: 12px;
			text-align: left;
			font-weight: bold;
		}}
		td {{
			padding: 10px 12px;
			border-bottom: 1px solid #ddd;
		}}
		tr:hover {{
			background-color: #f9f9f9;
		}}
		.amount {{
			text-align: right;
			font-weight: 500;
		}}
		.total {{
			font-weight: bold;
			background-color: #e8f0f8;
			border-top: 2px solid #1F4E79;
		}}
		.section-header {{
			background-color: #f0f0f0;
			font-weight: bold;
			font-size: 16px;
		}}
		.footer {{
			margin-top: 40px;
			padding-top: 20px;
			border-top: 1px solid #ddd;
			text-align: center;
			color: #666;
			font-size: 12px;
		}}
	</style>
</head>
<body>
	<div class="container">
		<div class="header">
			<h1>{entity_name}</h1>
			<h2>{entity_name_ar}</h2>
			<h2>Quarterly Financial Report</h2>
		</div>
		
		<div class="meta-info">
			<p><strong>Reporting Period:</strong> {start} to {end}</p>
			<p><strong>Currency:</strong> {currency} ({currency_symbol})</p>
			<p><strong>LEI:</strong> {entity_lei}</p>
			<p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
		</div>
		
		<!-- Hidden XBRL Contexts and Units -->
		<div style="display:none">
			<xbrli:xbrl>
				<link:schemaRef xlink:type="simple" xlink:href="ifrs-full_entry_point.xsd"/>
				<xbrli:context id="C_Instant">
					<xbrli:entity>
						<xbrli:identifier scheme="http://standards.iso.org/iso/17442">{entity_lei}</xbrli:identifier>
					</xbrli:entity>
					<xbrli:period>
						<xbrli:instant>{end}</xbrli:instant>
					</xbrli:period>
				</xbrli:context>
				<xbrli:context id="C_Duration">
					<xbrli:entity>
						<xbrli:identifier scheme="http://standards.iso.org/iso/17442">{entity_lei}</xbrli:identifier>
					</xbrli:entity>
					<xbrli:period>
						<xbrli:startDate>{start}</xbrli:startDate>
						<xbrli:endDate>{end}</xbrli:endDate>
					</xbrli:period>
				</xbrli:context>
				<xbrli:unit id="U_currency">
					<xbrli:measure>iso4217:{currency}</xbrli:measure>
				</xbrli:unit>
			</xbrli:xbrl>
		</div>
		
		<!-- Balance Sheet -->
		<div class="statement">
			<h3>Statement of Financial Position (Balance Sheet)</h3>
			<table>
				<thead>
					<tr>
						<th>Line Item</th>
						<th class="amount">Amount ({currency})</th>
					</tr>
				</thead>
				<tbody>
"""
	
	# Add balance sheet items
	balance_sheet_items = [
		('AssetsCurrent', 'Current Assets'),
		('AssetsNoncurrent', 'Non-current Assets'),
		('Assets', 'Total Assets'),
		('LiabilitiesCurrent', 'Current Liabilities'),
		('LiabilitiesNoncurrent', 'Non-current Liabilities'),
		('Liabilities', 'Total Liabilities'),
		('Equity', 'Equity'),
	]
	
	has_balance_sheet = False
	for concept_key, label in balance_sheet_items:
		fact_key = f'ifrs-full:{concept_key}'
		if fact_key in facts:
			value = facts[fact_key]
			context_ref = get_context_ref(fact_key)
			html += f"""
					<tr>
						<td>{label}</td>
						<td class="amount" ix:nonFraction="ifrs-full:{concept_key}" ix:contextRef="{context_ref}" ix:unitRef="U_currency" ix:format="num-dot-decimal" ix:decimals="-3">{format_value(value)}</td>
					</tr>
"""
			has_balance_sheet = True
	
	if not has_balance_sheet:
		html += f"""
					<tr>
						<td colspan="2" style="text-align: center; color: #666; font-style: italic;">No balance sheet data available</td>
					</tr>
"""
	
	html += f"""
				</tbody>
			</table>
		</div>
		
		<!-- Income Statement -->
		<div class="statement">
			<h3>Statement of Profit or Loss (Income Statement)</h3>
			<table>
				<thead>
					<tr>
						<th>Line Item</th>
						<th class="amount">Amount ({currency})</th>
					</tr>
				</thead>
				<tbody>
"""
	
	# Add income statement items
	income_statement_items = [
		('Revenue', 'Revenue'),
		('CostOfSales', 'Cost of Sales'),
		('GrossProfit', 'Gross Profit'),
		('OperatingExpenses', 'Operating Expenses'),
		('FinanceCosts', 'Finance Costs'),
		('FinanceIncome', 'Finance Income'),
		('IncomeTaxExpense', 'Income Tax Expense'),
		('ProfitLoss', 'Net Income'),
	]
	
	has_income_statement = False
	for concept_key, label in income_statement_items:
		fact_key = f'ifrs-full:{concept_key}'
		if fact_key in facts:
			value = facts[fact_key]
			context_ref = get_context_ref(fact_key)
			html += f"""
					<tr>
						<td>{label}</td>
						<td class="amount" ix:nonFraction="ifrs-full:{concept_key}" ix:contextRef="{context_ref}" ix:unitRef="U_currency" ix:format="num-dot-decimal" ix:decimals="-3">{format_value(value)}</td>
					</tr>
"""
			has_income_statement = True
	
	if not has_income_statement:
		html += f"""
					<tr>
						<td colspan="2" style="text-align: center; color: #666; font-style: italic;">No income statement data available</td>
					</tr>
"""
	
	html += f"""
				</tbody>
			</table>
		</div>
		
		<!-- Cash Flow Statement -->
		<div class="statement">
			<h3>Statement of Cash Flows</h3>
			<table>
				<thead>
					<tr>
						<th>Line Item</th>
						<th class="amount">Amount ({currency})</th>
					</tr>
				</thead>
				<tbody>
"""
	
	# Add cash flow items
	cash_flow_items = [
		('CashGeneratedFromOperations', 'Cash from Operations'),
		('CashFlowsFromUsedInInvestingActivities', 'Cash from Investing'),
		('CashFlowsFromUsedInFinancingActivities', 'Cash from Financing'),
		('CashAndCashEquivalentsAtEndOfPeriod', 'Net Change in Cash'),
	]
	
	has_cash_flow = False
	for concept_key, label in cash_flow_items:
		fact_key = f'ifrs-full:{concept_key}'
		if fact_key in facts:
			value = facts[fact_key]
			context_ref = get_context_ref(fact_key)
			html += f"""
					<tr>
						<td>{label}</td>
						<td class="amount" ix:nonFraction="ifrs-full:{concept_key}" ix:contextRef="{context_ref}" ix:unitRef="U_currency" ix:format="num-dot-decimal" ix:decimals="-3">{format_value(value)}</td>
					</tr>
"""
			has_cash_flow = True
	
	if not has_cash_flow:
		html += f"""
					<tr>
						<td colspan="2" style="text-align: center; color: #666; font-style: italic;">No cash flow data available</td>
					</tr>
"""
	
	html += f"""
				</tbody>
			</table>
		</div>
		
		<!-- GRC Financial Items (Other Items) -->
		<div class="statement">
			<h3>GRC Financial Items</h3>
			<table>
				<thead>
					<tr>
						<th>Line Item</th>
						<th class="amount">Amount ({currency})</th>
					</tr>
				</thead>
				<tbody>
"""
	
	# Add GRC/Other items that are not in standard statements
	grc_items = [
		('OtherExpenses', 'Other Expenses'),
		('ImpairmentLosses', 'Impairment Losses'),
		('OtherIncome', 'Other Income'),
		('Provisions', 'Provisions'),
		('Commitments', 'Commitments'),
	]
	
	# Track if we have any GRC items to display
	has_grc_items = False
	
	for concept_key, label in grc_items:
		fact_key = f'ifrs-full:{concept_key}'
		if fact_key in facts:
			value = facts[fact_key]
			context_ref = get_context_ref(fact_key)
			html += f"""
					<tr>
						<td>{label}</td>
						<td class="amount" ix:nonFraction="ifrs-full:{concept_key}" ix:contextRef="{context_ref}" ix:unitRef="U_currency" ix:format="num-dot-decimal" ix:decimals="-3">{format_value(value)}</td>
					</tr>
"""
			has_grc_items = True
	
	# If no GRC items found, show all remaining facts
	if not has_grc_items:
		# Show all facts that weren't displayed in standard statements
		displayed_facts = set()
		for items_list in [balance_sheet_items, income_statement_items, cash_flow_items]:
			for concept_key, _ in items_list:
				displayed_facts.add(f'ifrs-full:{concept_key}')
		
		for fact_key, value in facts.items():
			if fact_key not in displayed_facts:
				_, concept_key = fact_key.split(':', 1) if ':' in fact_key else ('ifrs-full', fact_key)
				label = get_concept_name(fact_key)
				context_ref = get_context_ref(fact_key)
				html += f"""
					<tr>
						<td>{label}</td>
						<td class="amount" ix:nonFraction="ifrs-full:{concept_key}" ix:contextRef="{context_ref}" ix:unitRef="U_currency" ix:format="num-dot-decimal" ix:decimals="-3">{format_value(value)}</td>
					</tr>
"""
	
	html += f"""
				</tbody>
			</table>
		</div>
		
		<div class="footer">
			<p>This document contains iXBRL (Inline XBRL) tags for machine-readable financial data.</p>
			<p>EGX | FRA | CBE | GAFI Compliant</p>
		</div>
	</div>
</body>
</html>
"""
	
	return html.encode('utf-8')


