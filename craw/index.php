<?php
// Xử lý POST request từ form
$result = null;
$error = null;
$debug = [];

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['url'])) {
    $url = $_POST['url'];
    
    if (!empty($url)) {
        $timestamp = date('Ymd_His');
        $outputFile = "output/batdongsan_{$timestamp}.json";
        
        // Python executable path (Laragon)
        $pythonPath = 'C:\laragon\bin\python\python-3.10\python.exe';
        
        // Set environment variables cho Python
        putenv('HOME=C:\laragon\www\craw');
        putenv('USERPROFILE=C:\laragon\www\craw');
        putenv('CRAWL4_AI_BASE_DIRECTORY=C:\laragon\www\craw\.crawl4ai');
        
        // Tạo thư mục .crawl4ai nếu chưa có
        if (!is_dir('.crawl4ai')) {
            mkdir('.crawl4ai', 0777, true);
        }
        
        // Chạy Python script với full path
        $command = "\"{$pythonPath}\" extract_batdongsan.py " . escapeshellarg($url) . " " . escapeshellarg($outputFile) . " 2>&1";
        exec($command, $output, $returnCode);
        
        $debug['command'] = $command;
        $debug['returnCode'] = $returnCode;
        $debug['output'] = $output;
        
        // Đợi file được tạo (tăng thời gian chờ)
        $maxWait = 15; // 15 giây
        $waited = 0;
        while (!file_exists($outputFile) && $waited < $maxWait) {
            sleep(1);
            $waited++;
        }
        
        $debug['waitedSeconds'] = $waited;
        $debug['fileExists'] = file_exists($outputFile);
        
        // Đọc kết quả
        if (file_exists($outputFile)) {
            $fileContent = file_get_contents($outputFile);
            $jsonData = json_decode($fileContent, true);
            
            if ($jsonData && isset($jsonData['data'])) {
                $result = $jsonData['data'];
            } else {
                $error = "Không parse được JSON";
            }
        } else {
            $error = "File không tồn tại sau {$waited}s. Check debug info below.";
        }
    } else {
        $error = "URL không được để trống";
    }
}
?>
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crawl Batdongsan.com.vn</title>
    <style>
        body {
            font-family: monospace;
            padding: 20px;
            background: #1e1e1e;
            color: #d4d4d4;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #4ec9b0;
        }
        .input-group {
            margin: 20px 0;
        }
        input {
            width: 70%;
            padding: 10px;
            font-family: monospace;
            font-size: 14px;
        }
        button {
            padding: 10px 20px;
            background: #0e639c;
            color: white;
            border: none;
            cursor: pointer;
            font-family: monospace;
            font-size: 14px;
        }
        button:hover {
            background: #1177bb;
        }
        button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        #status {
            margin: 20px 0;
            padding: 10px;
            background: #2d2d2d;
            border-left: 3px solid #4ec9b0;
        }
        #output {
            background: #1e1e1e;
            border: 1px solid #3e3e3e;
            padding: 20px;
            overflow: auto;
            max-height: 80vh;
        }
        pre {
            margin: 0;
            color: #ce9178;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏠 Crawl Batdongsan.com.vn</h1>
        
        <form method="POST" action="">
            <div class="input-group">
                <input type="text" name="url" placeholder="Nhập URL từ batdongsan.com.vn..." 
                       value="https://batdongsan.com.vn/ban-shophouse-nha-pho-thuong-mai-xa-da-ton-prj-the-paris-vinhomes-ocean-park/nhan-booking-shop-chan-de-vi-tri-vang-bac-nhat-1-so-luong-rat-gioi-han-pr44461179">
                <button type="submit">Crawl</button>
            </div>
        </form>
        
        <?php if ($error): ?>
            <div id="status" style="color: #f48771;">❌ Lỗi: <?php echo htmlspecialchars($error); ?></div>
            <?php if (!empty($debug)): ?>
                <div id="output">
                    <pre style="color: #dcdcaa;"><?php echo json_encode($debug, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT); ?></pre>
                </div>
            <?php endif; ?>
        <?php elseif ($result): ?>
            <div id="status" style="color: #4ec9b0;">✅ Crawl thành công!</div>
            <div id="output">
                <pre><?php echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT); ?></pre>
            </div>
        <?php endif; ?>
    </div>
</body>
</html>
