#
# from pytest_mock import MockerFixture
#
# from main import setup_mqtt_client
#
#
# def test_app(mocker: MockerFixture) -> None:
#     mock = mocker.Mock()
#     app.dependency_overrides[setup_mqtt_client] = lambda: mock
#     result = TestClient(app).post(
#         "/",
#         content=b"*HQ,9170119247,V6,102523,A,5628.0960,N,8502.8648,E,0.00,284.00,291024,FFFFFBFF,250,2,7006,50421,897011102513827006FF,#",
#         headers={"Content-Type": "application/x-www-form-urlencoded"},
#     )
#     assert result.content == b'"OK"'
#     mock.publish.assert_called_once()
