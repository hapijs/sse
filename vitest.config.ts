import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        environment: 'node',
        include: ['test/**/*.test.ts'],
        typecheck: {
            enabled: true,
            include: ['**/*.ts'],
        },
        coverage: {
            enabled: true,
            provider: 'v8',
            include: ['src/**/*.ts'],
            reportsDirectory: './coverage',
            reporter: ['text', 'lcov'],
            thresholds: {
                statements: 100,
                branches: 100,
                functions: 100,
                lines: 100
            },
        }
    }
});
