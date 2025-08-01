import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen } from "@testing-library/react";
import ErrorBoundary from "./error-boundary";

function SafeComponent() {
  return <div>Safe content</div>;
}

function CrashingComponent() {
  throw new Error("Test error");
  return null;
}

describe("ErrorBoundary", () => {
  let consoleLogger: any;

  beforeEach(() => {
    consoleLogger = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleLogger.mockRestore();
  });

  it("renders children when no error occurs", () => {
    render(
      <ErrorBoundary>
        <SafeComponent />
      </ErrorBoundary>,
    );

    expect(screen.getByText("Safe content")).toBeInTheDocument();
  });

  it("shows error message when child component crashes", () => {
    render(
      <ErrorBoundary>
        <CrashingComponent />
      </ErrorBoundary>,
    );

    expect(screen.getByText("Something went wrong.")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });

  it("logs error details for debugging", () => {
    render(
      <ErrorBoundary>
        <CrashingComponent />
      </ErrorBoundary>,
    );

    expect(consoleLogger).toHaveBeenCalledWith(
      "Uncaught error:",
      expect.any(Error),
      expect.any(Object),
    );
  });
});
