package nifi

import (
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
)

func TestNewClientRequiresBaseURL(t *testing.T) {
	_, err := NewClient(Config{ClientCertPEM: "x", ClientKeyPEM: "y"})
	if err == nil {
		t.Error("NewClient with no BaseURL should error")
	}
}

func TestNewClientRequiresClientCertAndKey(t *testing.T) {
	_, err := NewClient(Config{BaseURL: "https://example.invalid"})
	if err == nil {
		t.Error("NewClient with no client cert/key should error")
	}
}

func TestNewClientRejectsInvalidCertPEM(t *testing.T) {
	_, err := NewClient(Config{
		BaseURL: "https://example.invalid", ClientCertPEM: "not a cert", ClientKeyPEM: "not a key",
	})
	if err == nil {
		t.Error("NewClient with garbage cert/key PEM should error")
	}
}

// testEncryptedKeyPEM is a real RSA key generated for this test only
// (openssl genrsa -traditional -des3 -passout pass:testpass123),
// encrypted with the traditional OpenSSL "Proc-Type: 4,ENCRYPTED" PEM
// format - the same format NiFi toolkit's own generated client-key.pem
// uses before anyone strips the passphrase from a copy of it. Not used
// for anything beyond this test.
const testEncryptedKeyPEM = `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,5B0A77C348765E5B

Eamc8wq905Ki7es7P/iTA+SJuJ/UI/WO8n7Z0tAtD4yMs+51SDVEQyufE3xbl10v
yXZDNWq8Xy7PTr0kTjyl6F/+Tp0hsu1BHGNBqpjkqKR1y7+kPkQawLAyyazHCoGV
rglFXGevsCte9LC0OZ0iOOclnqyd3glgcpq7lWLSJSE8BwksxkaPe00TOwWqgBeu
ZyfGcdXtN67poQDMB5AqK0tX8v82KYTBO56mlzPqznxN7iqRmZDqodroy9ro+I8k
rmYCnkag/5likuGJj8/uBQUFx9805S2zUF2xAua8bGHKV46KUAVVNzFBbEL2ii7P
L7bZuC8H8Czy5i30sRGgOvqlWrsqKg9Fyu8bBbbVvRK6tmhjB18NyK6g9tpUT0Kd
6vJ3NdS0DbGx13ymg88DlcdYj+317oq1k+9F5YYUMj1qOg+/kDFko1r+k63kPTIq
L7KCDc1zR+dqfwmK290cfWYkxM2B9ZFwolCSIXmXQOiVWcHI2cle48ilFcdXJ/3D
lHQcY/EXXRU8zSkcH4zGPE5UhyJ70oi7BtB/XC2LEskLG69YOKdfqzSanBOO+DkP
WSBvy7M5dWTxaTHXiw44sQwHoNhVEUCsLqy3ClmzHXQbBLPDCWzvqLd3BN3lwCPB
rWWGmAji9IMJkDMYuTYKn8N3Y0s/4RPOqsLmJPjDrTVqCiiOgk97vUs0Do8J66DM
oWETEgzg0GGERU+/qTDMY9BwTm1Ro+oWXvPHkUKh0DMChITI0WOMcxUzMT2r0LLA
3vmAMaYBwZWq3grxXUH5d3e0bWhkBAvbjjEAULo/gvaXBB/ze/mG/Y7RiUnqF+i1
VAPOJ87S9n8wSmBpQpViP2YJeXoaZkanEDS381Q+yvt9GRc9R5KaD09JlpyeqoW6
rrQFBLdtiXwc0FJr/5gNd/29p4OewYNeH/gDbF4hMmIsjOvKFmHyBTnY+3Q5Kvfl
/TUsbZb5lD0BudyEm3FgQy8N+QnNNXilFKHKO7QjDQmHrAuub2dBdG06MA8ftk4v
/2MzlZ+XQ9kp3h6V0Wr7bDne9AerBSp2yAxB9Or+TOydX8M9nYmmpIGrzvo3V9Dz
DlKUfOrinuzlzXT0HAJh1wNZBmy8HB26oARUGn3G7zpdef5P1sAzxdYsLAzcStM4
xYyzcI2q4wXvghYo6COQxAjMynMEgsSPKleiKd5z+fPR07vwqxzZxGdtduJX5W2a
sVlPfCtAFCcoEYBNF7qgygvd/OlfBPPPmo7HYeIJfOQV/oTJsZzJPAhj5l+VtOVB
c2G0poioO3149oiBThEH3lLziDXCaW0RNy2qFi829DauUl8qoWz0nYWr6cL6WonE
FbbOenDNaau3LYl6lUO6d85yAPkZe9YfjzXM82yXXnEfrRdbL5WioSJCP+LOKiUh
YmzOyOkz7F+bkiJN8EbPknjbuSylQ+OVpIZAn+2KF+A+UFEka4f+uG8WjbRmHiXL
LuPcxqEJSYXqzJNeFJfGmy4RH7OI1kRCcxD1Pfjg8lrunkfjsjKtr1zAjLl/Ux/P
mvF0ZQcFayq2Wcu0XRI378YApo5OBJzAPA1okNf5rnpROzG4tF7zoA==
-----END RSA PRIVATE KEY-----
`

func TestDecryptKeyPEMIfNeededWithCorrectPassphrase(t *testing.T) {
	got, err := decryptKeyPEMIfNeeded([]byte(testEncryptedKeyPEM), "testpass123")
	if err != nil {
		t.Fatalf("decryptKeyPEMIfNeeded: %v", err)
	}
	if !strings.Contains(string(got), "-----BEGIN RSA PRIVATE KEY-----") || strings.Contains(string(got), "ENCRYPTED") {
		t.Errorf("decrypted output does not look like an unencrypted RSA key: %s", got)
	}
	// The decrypted key must actually be usable - pair it with a real
	// cert generated from it would be ideal, but simply confirming it
	// parses as a valid private key is enough to prove decryption
	// worked (a wrong passphrase produces garbage bytes that fail to
	// parse as ASN.1, not a plausible-looking-but-wrong key).
	block, _ := pem.Decode(got)
	if block == nil {
		t.Fatal("decrypted key did not parse as a PEM block")
	}
	if _, err := x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
		t.Errorf("decrypted key did not parse as a valid RSA private key: %v", err)
	}
}

func TestDecryptKeyPEMIfNeededWithWrongPassphraseErrors(t *testing.T) {
	if _, err := decryptKeyPEMIfNeeded([]byte(testEncryptedKeyPEM), "wrong-passphrase"); err == nil {
		t.Error("decryptKeyPEMIfNeeded with a wrong passphrase should error")
	}
}

func TestDecryptKeyPEMIfNeededRequiresPassphraseWhenEncrypted(t *testing.T) {
	if _, err := decryptKeyPEMIfNeeded([]byte(testEncryptedKeyPEM), ""); err == nil {
		t.Error("decryptKeyPEMIfNeeded with no passphrase for an encrypted key should error")
	}
}

func TestDecryptKeyPEMIfNeededLeavesUnencryptedKeyAlone(t *testing.T) {
	plain := []byte("-----BEGIN RSA PRIVATE KEY-----\nnot a real key, just needs to be valid-looking PEM\n-----END RSA PRIVATE KEY-----\n")
	got, err := decryptKeyPEMIfNeeded(plain, "some-passphrase-that-should-be-ignored")
	if err != nil {
		t.Fatalf("decryptKeyPEMIfNeeded on an unencrypted key should not error, got: %v", err)
	}
	if string(got) != string(plain) {
		t.Errorf("decryptKeyPEMIfNeeded modified an already-unencrypted key: got %q, want unchanged %q", got, plain)
	}
}

// testEncryptedPKCS8KeyPEM is a synthetic RSA key generated for this test
// only (openssl genrsa | openssl pkcs8 -topk8 -v2 aes-256-cbc), encrypted
// in the modern PKCS#8 "ENCRYPTED PRIVATE KEY" format - this is what a key
// extracted from a NiFi toolkit .p12 keystore via
// "openssl pkcs12 -nocerts -in nifi-monitor-bot.p12" looks like in
// practice, distinct from the traditional OpenSSL PEM encryption format
// covered by testEncryptedKeyPEM above. Not used for anything beyond this
// test.
const testEncryptedPKCS8KeyPEM = `-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI7aU6H/9hJ3UCAggA
MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBDIO1aeOZhCCjJhBYDQ/yWSBIIE
0Kkoq4wW8y3gRYCw7QSNqPmurV8J3338yKjy4ZHmzsMV+FrpcL6hw3MbnEMYmikm
XoVncVm6Dr9U9QvtgXI7yDLoar63OGfbLddfyeDs3AECsLTrCANGPsGGgmPiVGNZ
XzWKX/m+KRMTj2pW9+dZha1YISTuJ7EVdGN3LDGX0AM2We7XGATrtmHEaLoQLaQQ
VWnAqOIxszvY4pKFOrLP6mT44z6SHgR0OmnaNrgCp7Ey8gMHqpXWJu0AGGoCvlpc
8u6rUs2rMZfpylB/16pZIXA0FW9HhyiV/ogTRNao/uWqDADW9L2gYrKfxb+rN8vX
ZWUHUJRiI3X7y0KK5ApmkJ9GmVNguyzQ/2+fST2yxSKdhyFDIfVSPU6TuqgqCUaW
fgSrQsA7N/v1gzAOxb/7J3FscHnqYMQeW3jwOvphrw73GQFH+GCNCZTelBxXhxcB
KsAlZcCMTZMnuc7Bbh7Z8Y2QpJE5qcfJOfu2ONJtFwtdQgnAAS9BJn+HWuYUqsEd
rje8W33o8Aw7VTIoLxu1emKa5eaSs0TLEMyDRv1savJgejKLz4ObD3kcEXTGxlhz
OdQ0NbHkL3D0upwSIF2gQjIKSEUk7UcWwRFCzqTI5ZFEmvXyVGh1bGgAg6T/25c0
x6Y98tDDXsLh4FHNmVU8vpk39gWYvlXYji+SyfFwDaNNCJ9VoKWJsH8MiuAdAAsP
ph3hgjShHdAkfZyL1Qh20v3K6Fgpy1PN4WLGdgvAlbrBCuwXb7pondy3sf4kyMtZ
OW/GxDNJtseYfktHPOB2vZfhBWkt88egaZjX81R673mAkWqaTtROMMDiORVjU0GW
+AfZzr7a6ZnzW2JzhEday/OS/VtIYWxZyjKCwqXhpmZn65eSOeSsq4b3Qd78fZ/Y
90pZlynLFkK1zLRQ+8uFsItGsfvNwakgb3GbIczvy51o9QcOHeBzks9Le/aaU8nD
UCdXhRhCFW/sQjSzjdd+BJbOpa1P4iKGfaC70WqlLnnlCh+uL6BbTEorQyIDPNbF
ka6/epUwTulh9jPEWs9XHVop44yGgUHgBmZtvfyYi/xGIU/NtAbcH7b8/ObJ+RW7
FDWElm8aPkG4WEMpXYaVR/dpwqKYqTVTM9O0Kj7piViicYs9NHPnkWcvyFOB47Qt
YKUHuv8vy+B8kJa6bmknxTbw8vXO7voABxOaoOFjT1cL3J/MtxFjZY9G3wVq4fRy
f0WeKJ+NpSJ1kC22jpKwjLJUM7DGgYxxPSD2IOfo9KKN9m/saOTAUE5lHWRx7nhp
qVeHLc1htta0Os4EsClLWT0wbsuMw33TXIYx30sUZx0YDsQbte5sYYbca+79eKC4
VNX1Hkal2w9S0Jt/jUxSv4I0pzWEtKLN99grbbKFRo3KDKcBXDPdMOfsE6fxRRjZ
QFA9hGRAASC0/d9HUOal8GPuCr37NEUCJsWIpp7Aen7riQK2ENt1XEzZocc/m0Fu
OCxrWmZa11izeu+6ToHTMGQ7DjwZ3vRjhrK6AOwUqXuG80UafHfml6oAf21VoDLv
b4Op/3Q1W7Tl9/yA/tRCG7pOXgWavldVODx2LzW+W6ToJ4aHQ44UCynPhAraNJ7e
S4yk7tP/lvjjFPPfBuTI9olWYkI3aL+iBb7UDIUNNX8T
-----END ENCRYPTED PRIVATE KEY-----
`

func TestDecryptKeyPEMIfNeededPKCS8WithCorrectPassphrase(t *testing.T) {
	got, err := decryptKeyPEMIfNeeded([]byte(testEncryptedPKCS8KeyPEM), "testpass123")
	if err != nil {
		t.Fatalf("decryptKeyPEMIfNeeded: %v", err)
	}
	block, _ := pem.Decode(got)
	if block == nil {
		t.Fatal("decrypted output did not parse as a PEM block")
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		t.Errorf("decrypted output did not parse as a valid PKCS#8 private key: %v", err)
	}
}

func TestDecryptKeyPEMIfNeededPKCS8WithWrongPassphraseErrors(t *testing.T) {
	if _, err := decryptKeyPEMIfNeeded([]byte(testEncryptedPKCS8KeyPEM), "wrong-passphrase"); err == nil {
		t.Error("decryptKeyPEMIfNeeded with a wrong passphrase should error")
	}
}

func TestDecryptKeyPEMIfNeededPKCS8RequiresPassphraseWhenEncrypted(t *testing.T) {
	if _, err := decryptKeyPEMIfNeeded([]byte(testEncryptedPKCS8KeyPEM), ""); err == nil {
		t.Error("decryptKeyPEMIfNeeded with no passphrase for an encrypted PKCS#8 key should error")
	}
}
