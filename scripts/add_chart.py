#!/usr/bin/env python3
"""
Simple script to add charts with just SQL
Usage: python scripts/add_chart.py "Chart Name" "SELECT category as name, COUNT(*) as value FROM table GROUP BY category" "pie"
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.chart_registry import ChartConfig, add_chart

def main():
    if len(sys.argv) < 3:
        print("""
Usage: python scripts/add_chart.py "Chart Name" "SQL Query" [Chart Type]

Examples:
  python scripts/add_chart.py "Sales by Region" "SELECT region as name, SUM(amount) as value FROM sales GROUP BY region" "bar"
  python scripts/add_chart.py "User Status" "SELECT status as name, COUNT(*) as value FROM users GROUP BY status" "pie"
  python scripts/add_chart.py "Monthly Trend" "SELECT FORMAT(date, 'yyyy-MM') as name, COUNT(*) as value FROM events GROUP BY FORMAT(date, 'yyyy-MM') ORDER BY name" "line"

Chart Types: bar, pie, line, area, scatter (default: bar)
        """)
        sys.exit(1)

    name = sys.argv[1]
    sql = sys.argv[2]
    chart_type = sys.argv[3] if len(sys.argv) > 3 else 'bar'

    # Validate chart type
    valid_types = ['bar', 'pie', 'line', 'area', 'scatter']
    if chart_type not in valid_types:
        print(f"Error: Invalid chart type '{chart_type}'. Must be one of: {', '.join(valid_types)}")
        sys.exit(1)

    # Generate chart ID from name
    chart_id = name.lower().replace(' ', '-').replace('_', '-')
    chart_id = ''.join(c for c in chart_id if c.isalnum() or c == '-')
    chart_id = '-'.join(chart_id.split('-'))  # Remove consecutive dashes

    # Create chart configuration
    chart = ChartConfig(
        id=chart_id,
        name=name,
        chart_type=chart_type,
        sql=sql
    )

    # Add the chart
    success = add_chart(chart)
    
    if success:
        print(f"""
✅ Chart added successfully!

Chart ID: {chart_id}
Name: {name}
Type: {chart_type}
SQL: {sql}

The chart will be available at: /api/charts/{chart_id}
Dashboard data at: /api/charts/dashboard
        """)
        
        # Test the SQL (basic validation)
        if 'select' not in sql.lower():
            print("⚠️  Warning: SQL query should start with SELECT")
        
        if '{dateFilter}' not in sql:
            print("⚠️  Warning: Consider adding {dateFilter} for date filtering support")
            print("   Example: WHERE 1=1 {dateFilter}")
    else:
        print("❌ Failed to add chart")
        sys.exit(1)

if __name__ == "__main__":
    main()
