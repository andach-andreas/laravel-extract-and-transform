<?php

namespace Andach\ExtractAndTransform\Connectors\General\Excel;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use PhpOffice\PhpSpreadsheet\IOFactory;

class LegacyExcelConnector extends BaseConnector
{
    public function key(): string
    {
        return 'excel-legacy';
    }

    public function label(): string
    {
        return 'Excel (XLS/Legacy)';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', type: 'text', required: true, help: 'Absolute path to the XLS file.'),
        ];
    }

    public function test(array $config): void
    {
        $path = $config['path'] ?? '';
        if (! file_exists($path)) {
            throw new \RuntimeException("File not found at path: {$path}");
        }
    }

    public function datasets(array $config): array
    {
        $path = $config['path'] ?? '';
        $spreadsheet = IOFactory::load($path);

        $datasets = [];
        foreach ($spreadsheet->getSheetNames() as $index => $name) {
            $datasets[] = new RemoteDataset(identifier: $name, label: $name, meta: ['index' => $index]);
        }

        return $datasets;
    }

    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable
    {
        $path = $config['path'] ?? '';
        $spreadsheet = IOFactory::load($path);

        // Try to find sheet by identifier (name) first
        $sheet = $spreadsheet->getSheetByName($dataset->identifier);

        // Fallback to index if name not found, but only if index is explicitly provided
        // Or default to 0 if neither found?
        // Failing test relies on identifier being correct but index missing.
        // So getSheetByName should work.

        if ($sheet === null) {
             $sheet = $spreadsheet->getSheet($dataset->meta['index'] ?? 0);
        }

        $rows = $sheet->toArray();
        if (empty($rows)) {
            return;
        }

        $header = array_shift($rows); // Remove header

        foreach ($rows as $row) {
            $assocRow = [];
            foreach ($header as $i => $colName) {
                $assocRow[$colName] = $row[$i] ?? null;
            }
            yield $assocRow;
        }
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $path = $config['path'] ?? '';
        $spreadsheet = IOFactory::load($path);

        $sheet = $spreadsheet->getSheetByName($dataset->identifier);
        if ($sheet === null) {
            $sheet = $spreadsheet->getSheet($dataset->meta['index'] ?? 0);
        }

        $rows = $sheet->toArray();
        $header = $rows[0] ?? [];
        $firstRow = $rows[1] ?? [];

        $fields = [];
        foreach ($header as $i => $colName) {
            $val = $firstRow[$i] ?? null;
            $type = is_numeric($val) ? (str_contains((string)$val, '.') ? 'float' : 'int') : 'string';
            $fields[] = new RemoteField(name: (string)$colName, remoteType: 'string', nullable: true, suggestedLocalType: $type);
        }

        return new RemoteSchema($fields);
    }
}
