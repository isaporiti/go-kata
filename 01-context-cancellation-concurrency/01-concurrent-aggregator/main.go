package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	profileId := 1

	profiles := NewProfileService()
	profiles.profiles.Store(profileId, "Alice")

	orders := NewOrderService()
	orders.orders.Store(profileId, 5)

	logger := NewStdOutLogger()
	aggregator := NewUserAggregator(
		profiles,
		orders,
		WithLogger(logger),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aggregation, err := aggregator.Aggregate(ctx, profileId)
	if err != nil {
		logger.Error("could not aggregate user",
			"Id", profileId,
			"Error", err,
		)
		return
	}

	logger.Info("aggregation generated successfully",
		"Aggregation", aggregation,
	)
}

func NewStdOutLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(
			os.Stdout, nil,
		),
	)
}

type UserAggregator struct {
	profiles *profileService
	orders   *orderService
	logger   *slog.Logger
	timeout  time.Duration
}

type userAggregatorOption func(a *UserAggregator) error

func WithTimeout(t time.Duration) userAggregatorOption {
	return func(a *UserAggregator) error {
		if t <= 0 {
			return errors.New("timeout must be greater than 0")
		}

		a.timeout = t
		return nil
	}
}

func WithLogger(logger *slog.Logger) userAggregatorOption {
	return func(a *UserAggregator) error {
		if logger == nil {
			return errors.New("logger can't be nil")
		}

		a.logger = logger
		return nil
	}
}

func NewUserAggregator(
	profiles *profileService,
	orders *orderService,
	options ...userAggregatorOption,
) *UserAggregator {
	aggregator := &UserAggregator{
		profiles: profiles,
		orders:   orders,
		timeout:  0,
		logger:   NewStdOutLogger(),
	}
	for _, opt := range options {
		opt(aggregator)
	}
	return aggregator
}

func (a *UserAggregator) Aggregate(ctx context.Context, id int) (string, error) {
	var (
		eg      errgroup.Group
		cancel  context.CancelFunc
		profile profile
		orders  orders
	)

	if a.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	eg.Go(func() error {
		p, err := a.profiles.GetProfile(ctx, id)
		if err != nil {
			cancel()
			return err
		}

		profile = p
		return nil
	})
	eg.Go(func() error {
		o, err := a.orders.GetOrders(ctx, id)
		if err != nil {
			cancel()
			return err
		}

		orders = o
		return nil
	})
	if err := eg.Wait(); err != nil {
		a.logger.Error("could not aggregate user",
			"ProfileId", id,
			"Error", err,
		)
		return "", fmt.Errorf("an error occurred while aggregating user: %s", err)
	}

	return fmt.Sprintf(
		"User: %s | Orders: %d",
		profile.Name,
		orders.Quantity,
	), nil
}

type profileService struct {
	profiles sync.Map
}

func NewProfileService() *profileService {
	return &profileService{
		profiles: sync.Map{},
	}
}

func (p *profileService) GetProfile(ctx context.Context, id int) (profile, error) {
	select {
	case <-ctx.Done():
		return profile{}, ctx.Err()
	default:
	}

	if load, ok := p.profiles.Load(id); ok {
		if name, ok := load.(string); ok {
			return profile{name}, nil
		}
	}
	return profile{}, errors.New("profile not found")
}

type profile struct{ Name string }

type orderService struct {
	orders sync.Map
}

func NewOrderService() *orderService {
	return &orderService{
		orders: sync.Map{},
	}
}

func (o *orderService) GetOrders(ctx context.Context, profileId int) (orders, error) {
	select {
	case <-ctx.Done():
		return orders{}, ctx.Err()
	case <-time.After(10 * time.Second):
	}

	if load, ok := o.orders.Load(profileId); ok {
		if quantity, ok := load.(int); ok {
			return orders{quantity}, nil
		}
	}
	return orders{}, errors.New("order data not found")
}

type orders struct{ Quantity int }
