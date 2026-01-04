<?php

namespace Andach\ExtractAndTransform\Connectors;

final class ConnectorConfigDefinition
{
    public function __construct(
        public string $key,
        public string $label,
        public string $type = 'text',
        public bool $required = true,
        public ?string $help = null,
        public mixed $default = null
    ) {}
}
