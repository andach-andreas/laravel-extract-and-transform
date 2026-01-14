<?php

namespace Andach\ExtractAndTransform\Enrichment\Connectors;

use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Enrichment\Contracts\CanEnrich;
use Andach\ExtractAndTransform\Enrichment\Contracts\CanPreprocessIdentifier;
use Illuminate\Support\Facades\Http;

class CompaniesHouseConnector implements CanEnrich, CanPreprocessIdentifier
{
    public function key(): string
    {
        return 'companies_house';
    }

    public function label(): string
    {
        return 'UK Companies House';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'api_key', label: 'API Key', required: true, type: 'password'),
        ];
    }

    public function preprocessIdentifier(string|int $identifier): string|int
    {
        $processed = (string) $identifier;
        $processed = trim($processed);

        // Pad with leading zeros if necessary (UK company numbers are 8 digits)
        if (strlen($processed) < 8 && is_numeric($processed)) {
            $processed = str_pad($processed, 8, '0', STR_PAD_LEFT);
        }

        return $processed;
    }

    public function enrich(string|int $identifier, array $config): ?array
    {
        $apiKey = trim($config['api_key'] ?? '');

        if (empty($apiKey)) {
            throw new \RuntimeException('Companies House API Key is missing. Please check your enrichment profile configuration.');
        }

        // The identifier is now expected to be pre-processed by the EnrichmentService.
        $response = Http::withBasicAuth($apiKey, '')
            ->get("https://api.company-information.service.gov.uk/company/{$identifier}");

        if ($response->notFound()) {
            return null; // Company not found
        }

        if ($response->failed()) {
            throw new \RuntimeException("Companies House API Error ({$response->status()}): ".$response->body());
        }

        $data = $response->json();

        return [
            'company_name' => $data['company_name'] ?? null,
            'company_status' => $data['company_status'] ?? null,
            'registered_office_address' => json_encode($data['registered_office_address'] ?? []),
            'date_of_creation' => $data['date_of_creation'] ?? null,
            'sic_codes' => json_encode($data['sic_codes'] ?? []),
        ];
    }
}
