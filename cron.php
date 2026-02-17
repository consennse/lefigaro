<?php

$apiUrl = "https://lefigaro.onrender.com/run";

$data = [
    "source_url" => "https://manda.propertybase.com/api/v2/feed/00DWx000007hlhBMAQ/XML2U/a0hSb000005gQ01IAE/full",
    "ftp_host" => "ftp.figarocms.fr",
    "ftp_username" => "tld-maisonvictoire",
    "ftp_password" => "Jvexn^bF%4",
    "ftp_target_path" => "3374657.zip"
];

$ch = curl_init($apiUrl);

curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST => true,
    CURLOPT_POSTFIELDS => json_encode($data),
    CURLOPT_HTTPHEADER => ['Content-Type: application/json']
]);

$response = curl_exec($ch);

if (curl_errno($ch)) {
    echo "Error: " . curl_error($ch);
} else {
    echo $response;
}

curl_close($ch);
