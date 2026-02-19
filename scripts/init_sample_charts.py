#!/usr/bin/env python3
"""
Initialize sample charts for testing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.chart_registry import ChartConfig, add_chart

def main():
    print("🚀 Initializing sample charts...")

    # Sample chart 1: Sales by Region
    add_chart(ChartConfig(
        id='sales-by-region',
        name='Sales by Region',
        chart_type='bar',
        sql="""
        SELECT 
          CASE 
            WHEN region = 'North' THEN 'North America'
            WHEN region = 'South' THEN 'South America' 
            WHEN region = 'Europe' THEN 'Europe'
            WHEN region = 'Asia' THEN 'Asia Pacific'
            ELSE 'Other'
          END as name,
          SUM(amount) as value
        FROM (
          SELECT 'North' as region, 150000 as amount
          UNION ALL SELECT 'South', 120000
          UNION ALL SELECT 'Europe', 180000
          UNION ALL SELECT 'Asia', 200000
          UNION ALL SELECT 'Other', 50000
        ) sales
        WHERE 1=1 {dateFilter}
        GROUP BY region
        ORDER BY SUM(amount) DESC
        """
    ))

    # Sample chart 2: User Status Distribution
    add_chart(ChartConfig(
        id='user-status',
        name='User Status Distribution',
        chart_type='pie',
        sql="""
        SELECT 
          status as name,
          COUNT(*) as value
        FROM (
          SELECT 'Active' as status
          UNION ALL SELECT 'Active'
          UNION ALL SELECT 'Active'
          UNION ALL SELECT 'Inactive'
          UNION ALL SELECT 'Pending'
          UNION ALL SELECT 'Suspended'
        ) users
        WHERE 1=1 {dateFilter}
        GROUP BY status
        ORDER BY COUNT(*) DESC
        """
    ))

    # Sample chart 3: Monthly Growth Trend
    add_chart(ChartConfig(
        id='monthly-growth',
        name='Monthly Growth Trend',
        chart_type='line',
        sql="""
        SELECT 
          FORMAT(date, 'yyyy-MM') as name,
          COUNT(*) as value
        FROM (
          SELECT '2024-01-01' as date
          UNION ALL SELECT '2024-02-01'
          UNION ALL SELECT '2024-03-01'
          UNION ALL SELECT '2024-04-01'
          UNION ALL SELECT '2024-05-01'
          UNION ALL SELECT '2024-06-01'
          UNION ALL SELECT '2024-07-01'
          UNION ALL SELECT '2024-08-01'
          UNION ALL SELECT '2024-09-01'
          UNION ALL SELECT '2024-10-01'
          UNION ALL SELECT '2024-11-01'
          UNION ALL SELECT '2024-12-01'
        ) months
        WHERE 1=1 {dateFilter}
        GROUP BY FORMAT(date, 'yyyy-MM')
        ORDER BY name
        """
    ))

    # Sample chart 4: Product Categories
    add_chart(ChartConfig(
        id='product-categories',
        name='Product Categories',
        chart_type='bar',
        sql="""
        SELECT 
          category as name,
          COUNT(*) as value
        FROM (
          SELECT 'Electronics' as category
          UNION ALL SELECT 'Electronics'
          UNION ALL SELECT 'Electronics'
          UNION ALL SELECT 'Clothing'
          UNION ALL SELECT 'Clothing'
          UNION ALL SELECT 'Books'
          UNION ALL SELECT 'Home & Garden'
          UNION ALL SELECT 'Sports'
        ) products
        WHERE 1=1 {dateFilter}
        GROUP BY category
        ORDER BY COUNT(*) DESC
        """
    ))

    # Sample chart 5: Performance Score Distribution
    add_chart(ChartConfig(
        id='performance-scores',
        name='Performance Score Distribution',
        chart_type='scatter',
        sql="""
        SELECT 
          'Team A' as name,
          score as value
        FROM (
          SELECT 85 as score
          UNION ALL SELECT 92
          UNION ALL SELECT 78
          UNION ALL SELECT 88
          UNION ALL SELECT 95
          UNION ALL SELECT 82
          UNION ALL SELECT 90
          UNION ALL SELECT 87
        ) performance
        WHERE 1=1 {dateFilter}
        ORDER BY score DESC
        """
    ))

    print("✅ Sample charts initialized successfully!")
    print("📊 Available charts:")
    
    from shared.chart_registry import list_charts
    charts = list_charts()
    for chart in charts:
        print(f"   - {chart['name']} ({chart['type']})")

    print("""
🚀 Next steps:
1. Start your backend: python main.py
2. Visit: https://reporting-system-backend.pianat.ai/api/charts/dashboard
3. Or use the frontend: https://reporting-system-backend.pianat.ai/auto-dashboard
    """)

if __name__ == "__main__":
    main()
