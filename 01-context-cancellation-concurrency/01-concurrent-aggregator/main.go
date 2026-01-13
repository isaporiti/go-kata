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
	profiles := NewProfileService()
	profiles.profiles[1] = Profile{Name: "Alice"}

	orders := NewOrderService()
	orders.orders[1] = Orders{Quantity: 5}

	logger := NewStdOutLogger()
	aggregator, err := NewUserAggregator(
		profiles,
		orders,
		WithLogger(logger),
		WithTimeout(2*time.Second),
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// happy path
	{
		fmt.Println("*** HAPPY PATH ***")
		profileId := 1
		logger.Info("requesting user aggregation",
			"ProfileId", profileId,
		)
		aggregation, err := aggregator.Aggregate(ctx, profileId)
		if err != nil {
			logger.Error("could not aggregate user",
				"Id", profileId,
				"Error", err,
			)
			return
		}

		logger.Info("aggregation generated successfully",
			"ProfileId", profileId,
			"Aggregation", aggregation,
		)
	}

	// unhappy path
	{
		fmt.Println("\n*** UNHAPPY PATH (profile not found) **")
		profileId := -1
		logger.Info("requesting user aggregation",
			"ProfileId", profileId,
		)
		aggregation, err := aggregator.Aggregate(ctx, profileId)
		if err != nil {
			logger.Error("could not aggregate user",
				"Id", profileId,
				"Error", err,
			)
			return
		}

		logger.Info("aggregation generated successfully",
			"ProfileId", profileId,
			"Aggregation", aggregation,
		)
	}
}

func NewStdOutLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(
			os.Stdout, nil,
		),
	)
}

type UserAggregator struct {
	profiles *ProfileService
	orders   *OrderService
	logger   *slog.Logger
	timeout  time.Duration
}

type UserAggregatorOption func(a *UserAggregator) error

func WithTimeout(t time.Duration) UserAggregatorOption {
	return func(a *UserAggregator) error {
		if t <= 0 {
			return errors.New("timeout must be greater than 0")
		}

		a.timeout = t
		return nil
	}
}

func WithLogger(logger *slog.Logger) UserAggregatorOption {
	return func(a *UserAggregator) error {
		if logger == nil {
			return errors.New("logger can't be nil")
		}

		a.logger = logger
		return nil
	}
}

func NewUserAggregator(profiles *ProfileService, orders *OrderService, options ...UserAggregatorOption) (*UserAggregator, error) {
	aggregator := &UserAggregator{
		profiles: profiles,
		orders:   orders,
		timeout:  0,
		logger:   NewStdOutLogger(),
	}
	for _, opt := range options {
		err := opt(aggregator)
		if err != nil {
			return nil, err
		}
	}
	return aggregator, nil
}

func (a *UserAggregator) Aggregate(ctx context.Context, id int) (string, error) {
	var (
		eg      errgroup.Group
		cancel  context.CancelFunc
		profile Profile
		orders  Orders
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

type ProfileService struct {
	profiles map[int]Profile
	mux      sync.Mutex
}

func NewProfileService() *ProfileService {
	return &ProfileService{
		profiles: make(map[int]Profile),
		mux:      sync.Mutex{},
	}
}

func (p *ProfileService) GetProfile(ctx context.Context, id int) (Profile, error) {
	select {
	case <-ctx.Done():
		return Profile{}, ctx.Err()
	default:
	}

	p.mux.Lock()
	defer p.mux.Unlock()

	if profile, ok := p.profiles[id]; ok {
		return profile, nil
	}

	return Profile{}, errors.New("profile not found")
}

type Profile struct{ Name string }

type OrderService struct {
	orders map[int]Orders
	mux    sync.Mutex
}

func NewOrderService() *OrderService {
	return &OrderService{
		orders: make(map[int]Orders),
		mux:    sync.Mutex{},
	}
}

func (o *OrderService) GetOrders(ctx context.Context, profileId int) (Orders, error) {
	select {
	case <-ctx.Done():
		return Orders{}, ctx.Err()
	case <-time.After(1 * time.Second):
	}

	o.mux.Lock()
	defer o.mux.Unlock()

	if orders, ok := o.orders[profileId]; ok {
		return orders, nil
	}
	return Orders{}, errors.New("order data not found")
}

type Orders struct{ Quantity int }
