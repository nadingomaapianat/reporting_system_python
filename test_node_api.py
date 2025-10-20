import asyncio
import aiohttp

async def test_node_api():
    try:
        async with aiohttp.ClientSession() as session:
            # Test the Node.js API directly
            url = 'http://localhost:3001/api/grc/controls'
            async with session.get(url) as response:
                print(f'Node.js API status: {response.status}')
                if response.status == 200:
                    data = await response.json()
                    print(f'Node.js API data type: {type(data)}')
                    if isinstance(data, dict):
                        print(f'Node.js API data keys: {list(data.keys())}')
                        if 'controlsNotMappedToPrinciples' in data:
                            principles_data = data['controlsNotMappedToPrinciples']
                            print(f'controlsNotMappedToPrinciples found: {len(principles_data)} items')
                            if principles_data:
                                print(f'Sample item: {principles_data[0]}')
                    else:
                        print('Node.js API data is not a dict')
                else:
                    text = await response.text()
                    print(f'Node.js API error: {text}')
                    
    except Exception as e:
        print(f'Error testing Node.js API: {e}')

if __name__ == "__main__":
    asyncio.run(test_node_api())
