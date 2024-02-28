import pytest
from unittest.mock import mock_open, call, MagicMock
from app import fetch_data, export_data, transform_data  # Adjust import as necessary

def test_fetch_data(mocker):
    mock_connect = mocker.patch('mysql.connector.connect')
    mock_cursor = mock_connect().cursor()
    mock_cursor.fetchall.return_value = [('1', 'Hello', 'World', True)]
    mock_cursor.description = [('ID',), ('StringA',), ('StringB',), ('Flag',)]
    
    headers, data = fetch_data({
        'user': 'test',
        'password': 'test',
        'host': 'localhost',
        'database': 'test'
    })

    assert headers == ['ID', 'StringA', 'StringB', 'Flag']
    assert data == [('1', 'Hello', 'World', True)]
    mock_cursor.execute.assert_called_with("SELECT * FROM Data WHERE Flag = TRUE")
    mock_cursor.close.assert_called()
    mock_connect().close.assert_called()

def test_export_data(mocker):
    mock_file = mock_open()
    mocker.patch('builtins.open', mock_file)
    
    export_data('test.dat', ['ID', 'StringA', 'StringB', 'Flag'], [('1', 'Hello', 'World', True)])
    
    mock_file().write.assert_has_calls([
        call('ID,StringA,StringB,Flag\r\n'),
        call('1,Hello,World,True\r\n')
    ])

def test_transform_data(mocker):
    # Sample data to simulate what's read from 'exported.dat'
    read_data = 'ID,StringA,StringB,Flag\n1,Hello,World,1\n'

    # Mock open for both reading and writing. The first call to open is for reading, the second for writing.
    mock_file = mock_open(read_data=read_data)
    mocker.patch('builtins.open', mock_file)

    # Additional mock to simulate DictWriter behavior
    mock_writer = MagicMock()
    mocker.patch('csv.DictWriter', return_value=mock_writer)

    # Your function call
    transform_data('exported.dat', 'transformed.dat')

    # Assertions to ensure writeheader and writerow were called on DictWriter
    assert mock_writer.writeheader.called
    assert mock_writer.writerow.called

    # Verify the correct transformed data was written
    expected_write_calls = [
        mocker.call.writeheader(),
        mocker.call.writerow({'ID': '1', 'StringA': 'WORLD_', 'StringB': 'World', 'Flag': '1'})
    ]
    mock_writer.assert_has_calls(expected_write_calls, any_order=True)