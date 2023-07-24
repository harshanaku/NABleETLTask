import requests

url = "http://18.234.165.182:3050/rest/products/1/reviews"

# Set any required headers, such as authentication or content type
headers = {
    "Authorization": "Bearer <your_access_token>",
    "Content-Type": "application/json"
}

# Make the API call
response = requests.get(url, headers=headers)

# Check the response status code
if response.status_code == 200:
    # Assuming the response is in JSON format
    products = response.json()

    # Process the list of product details
    for product in products:
        print("Product Name:", product["name"])
        print("Product ID:", product["id"])
        print("Price:", product["price"])
        print("---")
else:
    print("Error:", response.status_code)
