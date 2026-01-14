<?php

namespace Andach\ExtractAndTransform\Connectors\General\Excel;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Storage;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Reader\Xls;

class LegacyExcelConnector extends BaseConnector implements CanInferSchema, CanStreamRows
{
    public function key(): string
    {
        return 'excel_legacy';
    }

    public function label(): string
    {
        return 'Legacy Excel (XLS)';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', required: true),
            new ConnectorConfigDefinition(key: 'disk', label: 'Storage Disk', required: false),
        ];
    }

    public function test(array $config): void
    {
        // getPath now performs the existence check
        $this->getPath($config);
    }

    public function datasets(array $config): iterable
    {
        $path = $this->getPath($config);

        // Use IOFactory to identify, but force XLS reader logic if needed
        $reader = new Xls;
        $reader->setReadDataOnly(true);
        $spreadsheet = $reader->load($path);

        foreach ($spreadsheet->getSheetNames() as $index => $name) {
            yield new RemoteDataset(
                identifier: $name,
                label: $name,
                meta: ['sheet_index' => $index]
            );
        }
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $path = $this->getPath($config);
        $reader = new Xls;
        $reader->setReadDataOnly(true);
        // Load only the specific sheet to save memory
        $reader->setLoadSheetsOnly($dataset->identifier);
        $spreadsheet = $reader->load($path);

        $sheet = $spreadsheet->getActiveSheet();
        $rows = $sheet->toArray();

        if (empty($rows)) {
            throw new \RuntimeException("Sheet '{$dataset->identifier}' is empty.");
        }

        $header = $rows[0];
        $firstRow = $rows[1] ?? [];

        $fields = [];
        foreach ($header as $index => $colName) {
            $val = $firstRow[$index] ?? null;
            $fields[] = new RemoteField(
                name: (string) $colName,
                remoteType: gettype($val),
                suggestedLocalType: $this->guessType($val),
                nullable: true
            );
        }

        return new RemoteSchema($fields);
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $path = $this->getPath($config);
        $reader = new Xls;
        $reader->setReadDataOnly(true);
        $reader->setLoadSheetsOnly($dataset->identifier);
        $spreadsheet = $reader->load($path);

        $sheet = $spreadsheet->getActiveSheet();

        // PhpSpreadsheet's toArray() loads everything into memory.
        // For true streaming of XLS, we'd need a different approach, but XLS files are usually smaller (65k row limit).
        // So loading into memory is acceptable for legacy support.

        $rows = $sheet->toArray();
        $header = array_shift($rows); // Remove header

        if (! $header) {
            return;
        }

        foreach ($rows as $row) {
            $assoc = [];
            foreach ($header as $i => $key) {
                $assoc[$key] = $row[$i] ?? null;
            }
            yield $assoc;
        }
    }

    private function getPath(array $config): string
    {
        $path = $config['path'] ?? null;

        if (empty($path)) {
            throw new \RuntimeException("Configuration is missing the required 'path' key.");
        }

        $disk = $config['disk'] ?? null;

        if ($disk) {
            if (! Storage::disk($disk)->exists($path)) {
                throw new \RuntimeException("File not found on disk '{$disk}': {$path}");
            }

            return Storage::disk($disk)->path($path);
        }

        if (! file_exists($path)) {
            throw new \RuntimeException("File not found at path: {$path}");
        }

        return $path;
    }

    private function guessType(mixed $val): string
    {
        if (is_int($val)) {
            return 'integer';
        }
        if (is_float($val)) {
            return 'decimal:18,4';
        }

        // PhpSpreadsheet returns dates as floats (Excel timestamp) unless configured otherwise.
        // We might need to handle that, but for now basic types.
        return 'string';
    }
}
