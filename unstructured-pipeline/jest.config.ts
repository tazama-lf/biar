import type { Config } from '@jest/types';

const config: Config.InitialOptions = {
  bail: 1,
  clearMocks: true,
  resetMocks: true,
  restoreMocks: true,

  preset: 'ts-jest',
  testEnvironment: 'node',

  roots: ['<rootDir>/__test__'],

  testMatch: ['**/*.spec.ts', '**/*.test.ts'],

  transform: {
    '^.+\\.ts$': 'ts-jest',
  },

  setupFiles: ['dotenv/config'],

  collectCoverage: true,
  collectCoverageFrom: ['src/job.ts'],
  coverageDirectory: '<rootDir>/coverage',
  coverageProvider: 'v8',

  coveragePathIgnorePatterns: [
    '/node_modules/',
    '/services/',
    '/interfaces/',
    '/types/',
  ],

  verbose: true,
};

export default config;
